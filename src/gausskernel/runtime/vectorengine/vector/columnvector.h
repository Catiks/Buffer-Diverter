#ifndef COLUMNVECTOR_H_
#define COLUMNVECTOR_H_

#include "postgres.h"
#include "access/tupdesc.h"
#include "utils/palloc.h"
#include "nodes/primnodes.h"
#include "nodes/bitmapset.h"

#define ColumnVectorSize                        1024
#define ColumnVectorSizeBit                     128
#define CacheLineSize                           64
#define CacheLineAlign(addr)                    (((uintptr_t)(addr) + ((CacheLineSize) - 1)) & ~((uintptr_t)((CacheLineSize) - 1)))
#define ColumnVectorStrDefaultSize              8192
#define FilterCalcSize                          64
#define Uint64Bit                               64
#define BatchIsEmpty(batch)                     ((batch) == NULL || (batch)->Empty())
#define DataOffsetGetValue(data, offset, idx)   (((uint8*)(data)) + ((offset)[((idx) - (1))]))
#define OffsetGetLen(offset, idx)               ((((uint32*)offset)[(idx)]) - (((uint32*)offset)[((idx) - (1))]))

typedef struct VecQualResult {
    uint32 count;
    uint32 left_count;
    uint32 rows;
    uint64* masks;
} VecQualResult;

extern bool VarLenTypeCheck(int typeId);

class CVector : public BaseObject {
    
public:
    CVector() = default;
    virtual ~CVector() {}

    /* For ColumnVector, ColumnVectorFixLen, ColumnVectorStr PushBack */
    virtual void Init(bool setzero = false) = 0;

    /* For ColumnVector, ColumnVectorFixLen, ColumnVectorStr ZeroCopy */
    virtual void InitByAddr(void* ptr, uint32 len, uint32 nulllen = 0, 
        uint32* offset = NULL, uint32 size = 0, uint8* bitmap = NULL) = 0;

    /* For ColumnVectorConst Init */
    virtual void InitByValue(const void* data, uint32 len, uint32 size = 0) = 0;

    virtual void PushBack(Datum value, uint32 size = 0) = 0;
    virtual void BatchPushBack(CVector* src, int32 start, int32 end) = 0;
    virtual void PushBackNULL() = 0;
    virtual void SetNULL(int32 idx, bool updatecount) = 0;
    virtual void SetNotNULL(int32 idx, bool updatecount) = 0;
    virtual void FlushNullLen() = 0;
    virtual uint32 Size() = 0;
    virtual uint32 NullSize() = 0;
    virtual void Clear() = 0;
    virtual bool Empty() = 0;
    virtual void Reset() = 0;
    virtual void Resize(uint32 len) = 0;
    virtual Datum Data() = 0;
    virtual void* DataAddr() = 0;
    virtual uint32* Offset() = 0;
    virtual void* OffsetAddr() = 0;
    virtual uint8* Bitmap() = 0;
    virtual void* BitmapAddr() = 0;
    virtual void VectorBatchPack(CVector* src, uint64* masks, uint32 count, int32* datapos) = 0;
    virtual void VectorOnePack(CVector* src, uint64 mask, int32* datapos) = 0;
    virtual void VectorPack(CVector* src, VecQualResult* QualResult, int32* datapos) = 0;
    virtual void FilterCopy(CVector* src, VecQualResult* QualResult) = 0;
    virtual void ShallowCopy(CVector* vector) = 0;
    virtual void DeepCopy(CVector* vector) = 0; 
    virtual Datum At(int32 idx) = 0;
    virtual uint32 AtSize(int32 idx) = 0;
    virtual bool AtSame(int32 idx, void* value, uint32 size = 0) = 0;
    virtual void PrefetchVectorData() = 0;
    virtual bool IsVarLenType() = 0;
    virtual void SetIsVarLenType(Oid typeId) = 0;
};

template <typename T>
class DataContainer : public BaseObject {

public:
    DataContainer() : _alloc_size(0), _start(NULL), _end(NULL), _cxt(NULL) {}

    FORCE_INLINE void* Alloc(MemoryContext cxt, uint32 size, bool setzero) {
        if (IsUnalloc()) {
            _alloc_size = size * sizeof(T);
            if (setzero)
                _start = (T*)MemoryContextAllocZero(cxt, size);
            else
                _start = (T*)MemoryContextAlloc(cxt, size);
            _end = _start;
            _cxt = cxt;
            return _start;
        }
        return NULL;
    }

    FORCE_INLINE void Realloc() {
        uint32 usedlen = _end - _start;
        _alloc_size *= 2;
        _start = (T*)repalloc(_start, _alloc_size);
        _end = _start + usedlen;
        return;
    }

    FORCE_INLINE void Free() {
        pfree(_start);
        _start = NULL;
        _end = NULL;
        _alloc_size = 0;
        _cxt = NULL;
    }

    FORCE_INLINE void SetData(T* addr, uint32 size) {
        _start = addr;
        _end = _start + size;
    }

    FORCE_INLINE void PushBack(const T* addr, const uint32 size) {
        memcpy(_end, addr, size);
        _end += size;
    }  

    FORCE_INLINE bool NoEnough(uint32 size) {
        return (_end - _start) + size > _alloc_size;
    }

    FORCE_INLINE T* Start() {
        return _start;
    }

    FORCE_INLINE void* DataAddr() {
        return &_start;
    }

    FORCE_INLINE T* End() {
        return _end;
    }

    FORCE_INLINE uint32 Size() {
        return _end - _start;
    }

    FORCE_INLINE void ShallowCopy(DataContainer<T>* dc) {
        _start = dc->_start;
        _end = dc->_end;
    }

    FORCE_INLINE void Reset() {
        _end = _start;
    }

    FORCE_INLINE bool IsUnalloc() {
        Assert(_alloc_size == 0);
        Assert(_cxt == NULL);
        return true;
    }

    FORCE_INLINE void Clear() {
        memset(_start, 0, _end - _start);
        return;
    }

private:
    uint32          _alloc_size;
    T*              _start;
    T*              _end;
    MemoryContext   _cxt;
};

template <typename T>
class ColumnVector : public CVector {

public:
    ColumnVector(MemoryContext cxt) : _len(0), _nulllen(0), _bitmap(NULL), _data(NULL), _cxt(cxt), _is_varlen_type(false) {}

    void Init(bool setzero = false) override;
    void InitByAddr(void* ptr, uint32 len, uint32 nulllen, uint32* offset = NULL, uint32 size = 0, uint8* bitmap = NULL) override; 
    void InitByValue(const void* data, uint32 len, uint32 size = 0) override;
    void VectorBatchPack(CVector* src, uint64* masks, uint32 count, int32* datapos) override;
    void VectorOnePack(CVector* src, uint64 mask, int32* datapos) override;
    void VectorPack(CVector* src, VecQualResult* QualResult, int32* datapos) override;
    void FilterCopy(CVector* src, VecQualResult* QualResult) override;
    void PushBack(Datum value, uint32 size = 0) override;
    void BatchPushBack(CVector* src, int32 start, int32 end) override;
    void PushBackNULL() override { _bitmap[_nulllen++] = 1; ++_len; }
    void SetNULL(int32 idx, bool updatecount) override { _bitmap[idx] = 1; _nulllen += updatecount; }
    void SetNotNULL(int32 idx, bool updatecount) override { _bitmap[idx] = 0; _nulllen -= updatecount; }
    void FlushNullLen() override { Assert(false); }
    void PrefetchVectorData() override;
    FORCE_INLINE uint32 Size() override { return _len; }
    FORCE_INLINE uint32 NullSize() override { return _nulllen; } 
    FORCE_INLINE void Clear() override;
    FORCE_INLINE bool Empty() override { return !_len; }
    FORCE_INLINE void Reset() override { _len = _nulllen = 0; }
    FORCE_INLINE void Resize(uint32 len) override { _len = len; }
    FORCE_INLINE Datum Data() override { return reinterpret_cast<Datum>(_data); }
    FORCE_INLINE void* DataAddr() override { return reinterpret_cast<void*>(&_data); }
    FORCE_INLINE uint32* Offset() override { return NULL; }
    FORCE_INLINE void* OffsetAddr() override { return NULL; }
    FORCE_INLINE uint8* Bitmap() override { return _bitmap; }
    FORCE_INLINE void* BitmapAddr() override { return reinterpret_cast<void*>(&_bitmap); }
    FORCE_INLINE void ShallowCopy(CVector* vector) override;
    FORCE_INLINE void DeepCopy(CVector* vector) override;
    FORCE_INLINE Datum At(int32 idx) override;
    FORCE_INLINE uint32 AtSize(int32 idx) override { return sizeof(T); }
    FORCE_INLINE bool AtSame(int32 idx, void* value, uint32 size = 0) override;
    FORCE_INLINE T AtInternal(int32 idx) { return (_data[idx]); }
    FORCE_INLINE void PushBackInternal(T & value) { _data[_len++] = value; }
    FORCE_INLINE bool IsVarLenType() override { return _is_varlen_type; }
    FORCE_INLINE void SetIsVarLenType(Oid typeId) override;
    uint32 GetFilterRows();
    uint32 GetFilterRowsSaveResult(VecQualResult* result);
    uint32 GetNoFilterRowsSaveResult(VecQualResult* result);

private:
    uint32          _len;
    uint32          _nulllen;
    uint8*          _bitmap;
    T*              _data;
    bool            _is_varlen_type;
    MemoryContext   _cxt;
};

template <uint32 unit>
class ColumnVectorFixLen : public CVector {

public:
    ColumnVectorFixLen(MemoryContext cxt) : _len(0), _nulllen(0), _bitmap(NULL), _data(NULL), _cxt(cxt) {}

    void Init(bool setzero = false) override;
    void InitByAddr(void* ptr, uint32 len, uint32 nulllen, uint32* offset = NULL, uint32 size = 0, uint8* bitmap = NULL) override;  
    void InitByValue(const void* data, uint32 len, uint32 size = 0) override;
    void VectorBatchPack(CVector* src, uint64* masks, uint32 count, int32* datapos) override;
    void VectorOnePack(CVector* src, uint64 mask, int32* datapos) override;
    void VectorPack(CVector* src, VecQualResult* QualResult, int32* datapos) override;
    void FilterCopy(CVector* src, VecQualResult* QualResult) override;
    void PushBack(Datum value, uint32 size = 0) override;
    void BatchPushBack(CVector* src, int32 start, int32 end) override;
    void PushBackNULL() override { _bitmap[_nulllen++] = 1; ++_len; }
    void SetNULL(int32 idx, bool updatecount) override { _bitmap[idx] = 1; _nulllen += updatecount; }
    void SetNotNULL(int32 idx, bool updatecount) override { _bitmap[idx] = 0; _nulllen -= updatecount; }
    void FlushNullLen() override { Assert(false); }
    void PrefetchVectorData() override { Assert(false); }
    FORCE_INLINE uint32 Size() override { return _len; }
    FORCE_INLINE uint32 NullSize() override { return _nulllen; }
    FORCE_INLINE void Clear() override;
    FORCE_INLINE bool Empty() override { return !_len; }
    FORCE_INLINE void Reset() override { _len = _nulllen = 0; }
    FORCE_INLINE void Resize(uint32 len) override { _len = len; }
    FORCE_INLINE Datum Data() override { return reinterpret_cast<Datum>(_data); }
    FORCE_INLINE void* DataAddr() override { return reinterpret_cast<void*>(&_data); }
    FORCE_INLINE uint32* Offset() override { return NULL; }
    FORCE_INLINE void* OffsetAddr() override { return NULL; }
    FORCE_INLINE uint8* Bitmap() override { return _bitmap; }
    FORCE_INLINE void* BitmapAddr() override { return reinterpret_cast<void*>(&_bitmap); }
    FORCE_INLINE void ShallowCopy(CVector* vector) override;
    FORCE_INLINE void DeepCopy(CVector* vector) override;
    FORCE_INLINE Datum At(int32 idx) override { return static_cast<Datum>(_data[idx * unit]); }
    FORCE_INLINE uint32 AtSize(int32 idx) override { return unit; }
    FORCE_INLINE bool AtSame(int32 idx, void* value, uint32 size = 0) override;
    FORCE_INLINE uint8* AtInternal(int32 idx) { return _data + (idx * unit); }
    FORCE_INLINE void PushBackInternal(uint8* value) { memcpy(_data + _len * unit, value, unit); ++_len; }
    FORCE_INLINE bool IsVarLenType() override { return false; }
    FORCE_INLINE void SetIsVarLenType(Oid typeId) override {};

private:
    uint32          _len;
    uint32          _nulllen;
    uint8*          _bitmap;
    uint8*          _data;
    MemoryContext   _cxt;
};

class ColumnVectorStr : public CVector {

public:
    using DT = uint8;
    using UI8 = DataContainer<DT>;

    ColumnVectorStr(MemoryContext cxt) : _len(0), _nulllen(0), _bitmap(NULL), _data(), _offset(NULL), _cxt(cxt) {}

    void Init(bool setzero = false) override;
    void InitByAddr(void* ptr, uint32 len, uint32 nulllen, uint32* offset, uint32 size, uint8* bitmap = NULL) override;
    void InitByValue(const void* data, uint32 len, uint32 size) override;
    void VectorBatchPack(CVector* src, uint64* masks, uint32 count, int32* datapos) override;
    void VectorOnePack(CVector* src, uint64 mask, int32* datapos) override;
    void VectorPack(CVector* src, VecQualResult* QualResult, int32* datapos) override;
    void FilterCopy(CVector* src, VecQualResult* QualResult) override { Assert(false); }
    void PushBack(Datum value, uint32 size) override;
    void BatchPushBack(CVector* src, int32 start, int32 end) override;
    void PushBackNULL() override { _bitmap[_nulllen++] = 1; ++_len; }
    void SetNULL(int32 idx, bool updatecount) override { _bitmap[idx] = 1; _nulllen += updatecount; }
    void SetNotNULL(int32 idx, bool updatecount) override { _bitmap[idx] = 0; _nulllen -= updatecount; }
    void FlushNullLen() override { Assert(false); }
    void PrefetchVectorData() override;
    FORCE_INLINE uint32 Size() override { return _len; }
    FORCE_INLINE uint32 NullSize() override { return _nulllen; }
    FORCE_INLINE void Clear() override;
    FORCE_INLINE bool Empty() override { return !_len; }
    FORCE_INLINE void Reset() override { _len = _nulllen = 0;_data.Reset(); }
    FORCE_INLINE void Resize(uint32 len) override { _len = len; }
    FORCE_INLINE Datum Data() override { return reinterpret_cast<Datum>(_data.Start()); }
    FORCE_INLINE void* DataAddr() override { return _data.DataAddr(); }
    FORCE_INLINE uint32* Offset() override { return _offset; }
    FORCE_INLINE void* OffsetAddr() override { return reinterpret_cast<void*>(&_offset); }
    FORCE_INLINE uint8* Bitmap() override { return _bitmap; }
    FORCE_INLINE void* BitmapAddr() override { return reinterpret_cast<void*>(&_bitmap); }
    FORCE_INLINE void ShallowCopy(CVector* vector) override;
    FORCE_INLINE void DeepCopy(CVector* vector) override;
    FORCE_INLINE Datum At(int32 idx) override { return reinterpret_cast<Datum>(_data.Start() + _offset[idx - 1]); }
    FORCE_INLINE uint32 AtSize(int32 idx) override { return _offset[idx] - _offset[idx - 1]; }
    FORCE_INLINE bool AtSame(int32 idx, void* value, uint32 size = 0) override;
    FORCE_INLINE DT* AtInternal(int32 idx) { return _data.Start() + _offset[idx - 1]; }
    FORCE_INLINE bool IsVarLenType() override { return true; }
    void SetIsVarLenType(Oid typeId) override {};

private:
    uint32          _len;
    uint32          _nulllen;
    uint8*          _bitmap;
    UI8             _data;
    uint32*         _offset;
    MemoryContext   _cxt;
};

template <typename T>
class ColumnVectorConst : public CVector {

public:
    ColumnVectorConst(MemoryContext cxt) : _len(0), _size(0), _data(0), _cxt(cxt), _is_varlen_type(false) {}

    void Init(bool setzero = false) override { return; }
    void InitByAddr(void* ptr, uint32 len, uint32 nulllen = 0, uint32* offset = NULL, uint32 size = 0, uint8* bitmap = NULL) override { Assert(false); }
    void InitByValue(const void* data, uint32 len, uint32 size = 0) override;
    void VectorBatchPack(CVector* src, uint64* masks, uint32 count, int32* datapos) override { Assert(false); }
    void VectorOnePack(CVector* src, uint64 mask, int32* datapos) override { Assert(false); }
    void VectorPack(CVector* src, VecQualResult* QualResult, int32* datapos) override { Assert(false); }
    void FilterCopy(CVector* src, VecQualResult* QualResult) override { Assert(false); }
    void PushBack(Datum value, uint32 size = 0) override { Assert(false); }
    void BatchPushBack(CVector* src, int32 start, int32 end) override { Assert(false); }
    void PushBackNULL() override { Assert(false); }
    void SetNULL(int32 idx, bool updatecount) override { Assert(false); }
    void SetNotNULL(int32 idx, bool updatecount) override { Assert(false); }
    void FlushNullLen() override { Assert(false); }
    void PrefetchVectorData() override { Assert(false); }
    FORCE_INLINE uint32 Size() override { return _len; }
    FORCE_INLINE uint32 NullSize() override { Assert(false); return 0; }
    FORCE_INLINE void Clear() override;
    FORCE_INLINE bool Empty() override { return !_len; }
    FORCE_INLINE void Reset() override { _len = 0; }
    FORCE_INLINE void Resize(uint32 len) override { _len = len; }
    inline Datum Data() override;
    FORCE_INLINE void* DataAddr() override { return reinterpret_cast<void*>(&_data); }
    FORCE_INLINE uint32* Offset() override {return &_size; }
    FORCE_INLINE void* OffsetAddr() override {return &_size; }
    FORCE_INLINE uint8* Bitmap() override { return NULL; }
    FORCE_INLINE void* BitmapAddr() override { return NULL; }
    FORCE_INLINE void ShallowCopy(CVector* vector) override;
    FORCE_INLINE void DeepCopy(CVector* vector) override;
    inline Datum At(int32 idx) override;
    FORCE_INLINE uint32 AtSize(int32 idx) override { return _size; }
    FORCE_INLINE bool AtSame(int32 idx, void* value, uint32 size = 0) override;
    FORCE_INLINE T AtInternal(uint32 idx) { return _data; }
    FORCE_INLINE bool IsVarLenType() override { return _is_varlen_type; }
    FORCE_INLINE void SetIsVarLenType(Oid typeId) override;

private:
    uint32          _len;
    uint32          _size;
    T               _data;
    bool            _is_varlen_type;
    MemoryContext   _cxt;
};

template <typename T>
inline Datum ColumnVectorConst<T>::Data() {
    return (Datum)_data;
}

template<> 
inline Datum ColumnVectorConst<float4>::Data() {
    return Float4GetDatum(_data);
}

template<> 
inline Datum ColumnVectorConst<float8>::Data() {
    return Float8GetDatum(_data);
}

template <typename T> 
inline Datum ColumnVectorConst<T>::At(int32 idx) {
    return (Datum)_data;
}

template<> 
inline Datum ColumnVectorConst<float4>::At(int32 idx) {
    return Float4GetDatum(_data);
}

template<> 
inline Datum ColumnVectorConst<float8>::At(int32 idx) {
    return Float8GetDatum(_data);
}

typedef class ColumnVector<int8>            ColumnVectorInt8;
typedef class ColumnVector<uint8>           ColumnVectorUint8;
typedef class ColumnVector<int16>           ColumnVectorInt16;
typedef class ColumnVector<int32>           ColumnVectorInt32;
typedef class ColumnVector<int64>           ColumnVectorInt64;
typedef class ColumnVector<float4>          ColumnVectorFloat4;
typedef class ColumnVector<float8>          ColumnVectorFloat8;
typedef class ColumnVectorFixLen<6>         ColumnVectorMacAddr;
typedef class ColumnVectorFixLen<12>        ColumnVectorTimeZone;
typedef class ColumnVectorFixLen<12>        ColumnVectorTInterval;
typedef class ColumnVectorFixLen<12>        ColumnVectorFloat4Array;
typedef class ColumnVectorFixLen<16>        ColumnVectorFloat8Array;
typedef class ColumnVectorFixLen<16>        ColumnVectorInterval;
typedef class ColumnVectorFixLen<16>        ColumnVectorUUID;
typedef class ColumnVectorFixLen<64>        ColumnVectorName;
typedef class ColumnVectorStr               ColumnVectorStr;
typedef class ColumnVectorConst<int8>       ColumnVectorConstInt8;
typedef class ColumnVectorConst<int16>      ColumnVectorConstInt16;
typedef class ColumnVectorConst<int32>      ColumnVectorConstInt32;
typedef class ColumnVectorConst<int64>      ColumnVectorConstInt64;
typedef class ColumnVectorConst<float4>     ColumnVectorConstFloat4;
typedef class ColumnVectorConst<float8>     ColumnVectorConstFloat8;
typedef class ColumnVectorConst<uint8*>     ColumnVectorConstStr;

class BatchVector : public BaseObject {

public:
    BatchVector(MemoryContext cxt, TupleDesc desc, Bitmapset* AssignColumn = NULL, bool NeedAlign = true);
    ~BatchVector() {}

    inline uint32 Cols() { return _cols; }
    inline uint32 Rows() { return _rows; }
    inline CVector* GetColumn(uint32 col) { return _vectors[col]; }
    inline void SetRows(uint32 rows) { _rows = rows; }
    inline bool Empty() { return !_rows; }

    void Init() {
        for (uint32 i = 0; i < _cols; ++i) {
            if (_vectors[i])
                _vectors[i]->Init();
        } 
    }

    void Reset() {
        for (uint32 i = 0; i < _cols; ++i) {
            if (_vectors[i])
                _vectors[i]->Reset();
        }
        _rows = 0; 
    }

    void ShallowCopy(BatchVector* batch) {
        for (uint32 i = 0; i < this->Cols(); ++i) {
            this->GetColumn(i)->ShallowCopy(batch->GetColumn(i));
        }
        this->SetRows(batch->Rows());
    }

    void DeepCopy(BatchVector* batch) {
        for (uint32 i = 0; i < this->Cols(); ++i) {
            this->GetColumn(i)->DeepCopy(batch->GetColumn(i));
        }
        this->SetRows(batch->Rows());
    }

private:
    uint32  _cols;
    uint32  _rows;
    CVector** _vectors;
};


CVector* AllocColumnVectorByType(MemoryContext cxt, int32 typeId);
CVector* CreateColumnVectorConst(MemoryContext cxt, Const *con);
void ColumnVectorUint8And(ColumnVectorUint8* leftarg, ColumnVectorUint8* rightarg, ColumnVectorUint8* result);
void ColumnVectorUint8Or(ColumnVectorUint8* leftarg, ColumnVectorUint8* rightarg, ColumnVectorUint8* result);
void ColumnVectorUint8Not(ColumnVectorUint8* arg, ColumnVectorUint8* result);
bool ColumnVectorIsFixLen(int32 typeId);
uint64 bytes64MaskToBits64Mask(const uint8 * bytes64);

inline uint64 blsr(uint64 mask)
{
    return mask & (mask - 1);
}

static constexpr uint8_t Bitmask[] = {1, 2, 4, 8, 16, 32, 64, 128};
static constexpr uint8_t FlippedBitmask[] = {254, 253, 251, 247, 239, 223, 191, 127};

inline bool GetBit(uint8* bits, uint32 i) 
{
    return (bits[i >> 3] >> (i & 0x07)) & 1;
}

inline void ClearBit(uint8* bits, uint32 i) 
{
    bits[i / 8] &= FlippedBitmask[i & 0x07];
}

inline void SetBit(uint8* bits, int64_t i) 
{ 
    bits[i / 8] |= Bitmask[i & 0x07]; 
}

/* Prevent implicit template instantiation of ColumnVector for common types */
extern template class ColumnVector<int8>;
extern template class ColumnVector<uint8>;
extern template class ColumnVector<int16>;
extern template class ColumnVector<int32>;
extern template class ColumnVector<int64>;
extern template class ColumnVectorFixLen<6>;
extern template class ColumnVectorFixLen<12>;
extern template class ColumnVectorFixLen<16>;
extern template class ColumnVectorFixLen<64>;
extern template class ColumnVectorConst<int8>;
extern template class ColumnVectorConst<uint8>;
extern template class ColumnVectorConst<int16>;
extern template class ColumnVectorConst<int32>;
extern template class ColumnVectorConst<int64>;

#endif /* COLUMNVECTOR_H_ */
