
#include "utils/aset.h"
#include "utils/memutils.h"
#include "nodes/primnodes.h"
#include "vecexecutor/columnvector.h"
#include "vecexecutor/vectorbatch.h"

#if defined(_M_AMD64) || defined(_M_X64) || defined(__amd64) || defined(__x86_64__)
#include <emmintrin.h>
#include <immintrin.h>
#elif defined(_M_ARM64) || defined(__aarch64__)
#include <arm_neon.h>
#else  /* none */
#endif

inline static uint8 prefixToCopy(uint64 mask);
inline static uint8 suffixToCopy(uint64 mask);

/* Explicit template instantiations - to avoid code bloat in headers */
template class ColumnVector<int8>;
template class ColumnVector<uint8>;
template class ColumnVector<int16>;
template class ColumnVector<int32>;
template class ColumnVector<int64>;
template class ColumnVector<float4>;
template class ColumnVector<float8>;
template class ColumnVectorFixLen<6>;
template class ColumnVectorFixLen<12>;
template class ColumnVectorFixLen<16>;
template class ColumnVectorFixLen<64>;
template class ColumnVectorConst<int8>;
template class ColumnVectorConst<int16>;
template class ColumnVectorConst<int32>;
template class ColumnVectorConst<int64>;
template class ColumnVectorConst<float4>;
template class ColumnVectorConst<float8>;
template class ColumnVectorConst<uint8*>;

/* ColumnVector */

template <typename T>
void ColumnVector<T>::Init() {
    Assert(_bitmap == NULL);
    Assert(_data == NULL);
    _len = 0;
    _nulllen = 0;
    _bitmap = (uint8*)MemoryContextAlloc(_cxt, sizeof(uint8) * ColumnVectorSize);
    _data = (T*)MemoryContextAlloc(_cxt, sizeof(T) * ColumnVectorSize);
}

template <typename T>
void ColumnVector<T>::InitByAddr(void* ptr, uint32 len, uint32 nulllen, uint32* offset, uint32 size, uint8* bitmap) {
    Assert(ptr);
    Assert(nulllen == 0 || (nulllen > 0 && bitmap));
    _len = len;
    _nulllen = nulllen;
    _bitmap = bitmap;
    _data = (T*)ptr;
}

template <typename T>
void ColumnVector<T>::InitByValue(const void* data, uint32 len, uint32 size) {
    Assert(data);
    Assert(len);
    _len = len;
    _nulllen = 0;
    _bitmap = NULL;
    for (uint32 i = 0; i < _len; ++i) {
        _data[i] = *((T*)data);
    }
}

template <typename T>
void ColumnVector<T>::PushBack(Datum value, uint32 size) {
    _data[_len++] = (T)value;
}

template <>
void ColumnVector<float4>::PushBack(Datum value, uint32 size) { 
    _data[_len++] = DatumGetFloat4(value);
}

template <>
void ColumnVector<float8>::PushBack(Datum value, uint32 size) { 
    _data[_len++] = DatumGetFloat8(value);
}

template <typename T>
void ColumnVector<T>::BatchPushBack(CVector* src, int32 start, int32 end) {
    ColumnVector<T>* v = static_cast<ColumnVector<T>*>(src);
    uint32 len = end - start;
    
    if (likely(len)) {
        memcpy(&_data[_len], &v->_data[start], len * sizeof(T));
        _len += len;
    }
}

template <typename T>
void ColumnVector<T>::ShallowCopy(CVector* vector) {
    ColumnVector<T> *v = static_cast<ColumnVector<T>*>(vector);
    _len = v->_len;
    _nulllen = v->_nulllen;
    _bitmap = v->_bitmap;
    _data = v->_data;
}

template <typename T>
void ColumnVector<T>::DeepCopy(CVector* vector) {
    ColumnVector<T>* v = static_cast<ColumnVector<T>*>(vector);
    _len = v->_len;
    _nulllen = v->_nulllen;
    if (_nulllen) {
        Assert(_bitmap);
        memcpy(_bitmap, v->_bitmap, _len * sizeof(uint8));
    }
    memcpy(_data, v->_data, _len * sizeof(T));
}

template <typename T>
void ColumnVector<T>::VectorBatchPack(CVector* src, uint64* masks, uint32 count, int32* datapos) {
    ColumnVector<T>* vsrc = static_cast<ColumnVector<T>*>(src);
    T* dsrc = reinterpret_cast<T*>(vsrc->Data());

    for (uint32 i = 0;i < count; ++i) {
        const uint8_t prefix_to_copy = prefixToCopy(masks[i]);
        if (0xFF != prefix_to_copy) {
            this->BatchPushBack(src, *datapos, *datapos + prefix_to_copy);
        }
        else {
            const uint8_t suffix_to_copy = suffixToCopy(masks[i]);
            if (0xFF != suffix_to_copy) {
                this->BatchPushBack(src, *datapos + FilterCalcSize - suffix_to_copy, *datapos + FilterCalcSize);
            }
            else {
                uint64 imask = masks[i];
                size_t index;
                while (imask) {
                    index = *datapos + __builtin_ctzll(imask);
                    this->PushBackInternal(dsrc[index]);
                    imask = blsr(imask);
                }
            }
        }
        *datapos += FilterCalcSize;
    }

    return;
}

template <typename T>
void ColumnVector<T>::VectorOnePack(CVector* src, uint64 mask, int32* datapos) {
    ColumnVector<T>* vsrc = static_cast<ColumnVector<T>*>(src);
    T* dsrc = reinterpret_cast<T*>(vsrc->Data());
    uint64 imask = mask;
    size_t index;
    
    while (imask) {
        index = *datapos + __builtin_ctzll(imask);
        this->PushBackInternal(dsrc[index]);
        imask = blsr(imask);
    }

    return;
}

template <typename T>
void ColumnVector<T>::PrefetchVectorData() {
    uint32 count = (_len * sizeof(T)) / CacheLineSize;
    uint32 offset = 0;

    for (uint32 i = 0;i < count; ++i) {
        __builtin_prefetch(_data + offset, 0, 2);
        offset += CacheLineSize;
    }
}

template <typename T>
void ColumnVector<T>::VectorPack(CVector* src, VecQualResult* QualResult, int32* datapos) {
    ColumnVector<T>* vsrc = static_cast<ColumnVector<T>*>(src);
    T* dsrc = reinterpret_cast<T*>(vsrc->Data());
    uint32 count, left_count, leftrows, index;
    int32 pos;
    uint64 mask;

    if (unlikely(_len == ColumnVectorSize))
        return;

    count = *datapos / FilterCalcSize;
    left_count = *datapos & (FilterCalcSize - 1);
    pos = count * FilterCalcSize;
    Assert(count <= QualResult->count);

    if (left_count)
        mask = (((uint64)-1) << left_count) & QualResult->masks[count];
    else
        mask = QualResult->masks[count];

    if (unlikely(count == QualResult->count))
        goto leftpack;

    while (true) {
        if (unlikely(_len + __builtin_popcountll(mask) > ColumnVectorSize)) {
            leftrows = ColumnVectorSize - _len;
            index = pos - 1;
            while (leftrows) {
                index = pos + __builtin_ctzll(mask);
                this->PushBackInternal(dsrc[index]);
                mask = blsr(mask);
                leftrows--;
            }
            *datapos += index - pos + 1;
            return;
        }

        const uint8_t prefix_to_copy = prefixToCopy(mask);
        if (0xFF != prefix_to_copy) {
            this->BatchPushBack(src, pos, pos + prefix_to_copy);
        }
        else {
            const uint8_t suffix_to_copy = suffixToCopy(mask);
            if (0xFF != suffix_to_copy) {
                this->BatchPushBack(src, pos + FilterCalcSize - suffix_to_copy, pos + FilterCalcSize);
            }
            else {
                while (mask) {
                    index = pos + __builtin_ctzll(mask);
                    this->PushBackInternal(dsrc[index]);
                    mask = blsr(mask);
                }
            }
        }

        *datapos += FilterCalcSize - left_count;
        left_count = 0;
        pos += FilterCalcSize; 
        ++count;
        Assert(*datapos % FilterCalcSize == 0);

        if (count == QualResult->count)
            break;
        mask = QualResult->masks[count]; 
    }
    mask = QualResult->masks[count];

leftpack:
    if (unlikely(QualResult->left_count)) {
        if (unlikely(_len + __builtin_popcountll(mask) > ColumnVectorSize)) {
            leftrows = ColumnVectorSize - _len;
            index = pos - 1;
            while (leftrows) {
                index = pos + __builtin_ctzll(mask);
                this->PushBackInternal(dsrc[index]);
                mask = blsr(mask);
                leftrows--;
            }
            *datapos += index - pos + 1;
        }
        else {
            while (mask) {
                index = pos + __builtin_ctzll(mask);
                this->PushBackInternal(dsrc[index]);
                mask = blsr(mask);
            }
            *datapos += QualResult->left_count - left_count;
        }
    }

    return;
}

template <typename T>
bool ColumnVector<T>::AtSame(int idx, void* value, uint32 size) {
    return *(T*)value == _data[idx];
}

template <typename T>
Datum ColumnVector<T>::At(int32 idx) { 
    return static_cast<Datum>(_data[idx]);
}

template<> 
Datum ColumnVector<float4>::At(int32 idx) { 
    return Float4GetDatum(_data[idx]); 
}

template<> 
Datum ColumnVector<float8>::At(int32 idx) { 
    return Float8GetDatum(_data[idx]); 
}

template <>
uint32 ColumnVector<uint8>::GetFilterRows() 
{
    uint32 i;
    uint64 mask;
	uint32 count = _len / FilterCalcSize;
	uint32 left_count = _len % FilterCalcSize;
    uint32 rows = 0;

    for (i = 0;i < count; ++i) {
        mask = bytes64MaskToBits64Mask(_data + i * FilterCalcSize);
        rows += __builtin_popcountll(mask);
    }

    if (unlikely(left_count)) {
        uint8* left_saddr = _data + FilterCalcSize * count;
        memset(left_saddr + left_count, 0, FilterCalcSize - left_count);
        mask = bytes64MaskToBits64Mask(left_saddr);
        rows += __builtin_popcountll(mask);   
    }

    return rows;
}

template <>
uint32 ColumnVector<uint8>::GetFilterRowsSaveResult(VecQualResult* result) 
{
    uint32 i;
	result->count = _len / FilterCalcSize;
	result->left_count = _len & (FilterCalcSize - 1);
    result->rows = 0;

    for (i = 0; i < result->count; ++i) {
        result->masks[i] = bytes64MaskToBits64Mask(_data + i * FilterCalcSize);
        result->rows += __builtin_popcountll(result->masks[i]);
    }

    if (unlikely(result->left_count)) {
        uint8* left_saddr = _data + (FilterCalcSize * (result->count));
        memset(left_saddr + result->left_count, 0, FilterCalcSize - (result->left_count));
        result->masks[i] = bytes64MaskToBits64Mask(left_saddr);
        result->rows += __builtin_popcountll(result->masks[i]);   
    }

    return result->rows;
}

void ColumnVectorUint8And(ColumnVectorUint8* leftarg, ColumnVectorUint8* rightarg, ColumnVectorUint8* result) 
{
    uint32 len = result->Size();
    uint8* a = reinterpret_cast<uint8*>(leftarg->Data());
    uint8* b = reinterpret_cast<uint8*>(rightarg->Data());
    uint8* c = reinterpret_cast<uint8*>(result->Data());

    for (uint32 i = 0; i < len; ++i) {
        //_mm_store_si128((reinterpret_cast<__m128i *>(&c[i])), 
        //    _mm_and_si128(
        //    _mm_loadu_si128(reinterpret_cast<const __m128i *>(&a[i])), 
        //    _mm_loadu_si128(reinterpret_cast<const __m128i *>(&b[i]))));  
        c[i] = a[i] & b[i];
    }
}

void ColumnVectorUint8Or(ColumnVectorUint8* leftarg, ColumnVectorUint8* rightarg, ColumnVectorUint8* result) 
{
    uint32 len = result->Size();
    uint8* a = reinterpret_cast<uint8*>(leftarg->Data());
    uint8* b = reinterpret_cast<uint8*>(rightarg->Data());
    uint8* c = reinterpret_cast<uint8*>(result->Data());

    for (uint32 i = 0; i < len; ++i) {
        c[i] = a[i] | b[i];
    }
}

/* ColumnVectorFixLen */

template <uint32 unit>
void ColumnVectorFixLen<unit>::Init() {
    Assert(_bitmap == NULL);
    Assert(_data == NULL);
    _len = 0;
    _nulllen = 0;
    _bitmap = (uint8*)MemoryContextAlloc(_cxt, sizeof(uint8) * ColumnVectorSize);
    _data = (uint8*)MemoryContextAlloc(_cxt, unit * ColumnVectorSize);
}

template <uint32 unit>
void ColumnVectorFixLen<unit>::InitByAddr(void* ptr, uint32 len, uint32 nulllen, uint32* offset, uint32 size, uint8* bitmap) {
    Assert(ptr);
    Assert(nulllen == 0 || (nulllen > 0 && bitmap));
    _len = len; 
    _nulllen = nulllen;
    _bitmap = bitmap;
    _data = (uint8*)ptr;
}

template <uint32 unit>
void ColumnVectorFixLen<unit>::InitByValue(const void* data, uint32 len, uint32 size) {
    Assert(data);
    Assert(len);
    _len = len;
    _nulllen = 0;
    _bitmap = NULL;

    for (uint32 i = 0; i < _len; ++i) {
        memcpy(_data + i * unit, data, unit);
    }
}

template <uint32 unit>
void ColumnVectorFixLen<unit>::PushBack(Datum value, uint32 size) {
    memcpy(_data + _len * unit,  reinterpret_cast<void*>(value), unit);
    ++_len;
}

template <uint32 unit>
void ColumnVectorFixLen<unit>::BatchPushBack(CVector* src, int32 start, int32 end) {
    ColumnVectorFixLen<unit>* v = static_cast<ColumnVectorFixLen<unit>*>(src);
    uint32 len = end - start;

    if (likely(len)) {
        memcpy(_data + _len * unit, v->_data + start * unit, len * unit);
        _len += len;
    }
}

template <uint32 unit>
void ColumnVectorFixLen<unit>::ShallowCopy(CVector* vector) {
    ColumnVectorFixLen<unit> *v = static_cast<ColumnVectorFixLen<unit>*>(vector);
    _len = v->_len;
    _nulllen = v->_nulllen;
    _bitmap = v->_bitmap;
    _data = v->_data;
}

template <uint32 unit>
void ColumnVectorFixLen<unit>::DeepCopy(CVector* vector) {
    ColumnVectorFixLen<unit> *v = static_cast<ColumnVectorFixLen<unit>*>(vector);
    _len = v->_len;
    if (_nulllen) {
        Assert(_bitmap);
        memcpy(_bitmap, v->_bitmap, _len * sizeof(uint8));
    }
    memcpy(_data, v->_data, _len * unit);
}

template <uint32 unit>
void ColumnVectorFixLen<unit>::VectorBatchPack(CVector* src, uint64* masks, uint32 count, int32* datapos) {
    ColumnVectorFixLen<unit>* vsrc = static_cast<ColumnVectorFixLen<unit>*>(src);

    for (uint32 i = 0;i < count; ++i) {
        const uint8_t prefix_to_copy = prefixToCopy(masks[i]);
        if (0xFF != prefix_to_copy) {
            this->BatchPushBack(src, *datapos, *datapos + prefix_to_copy);
        }
        else {
            const uint8_t suffix_to_copy = suffixToCopy(masks[i]);
            if (0xFF != suffix_to_copy) {
                this->BatchPushBack(src, *datapos + FilterCalcSize - suffix_to_copy, *datapos + FilterCalcSize);
            }
            else {
                uint64 imask = masks[i];
                size_t index;
                while (imask) {
                    index = *datapos + __builtin_ctzll(imask);
                    this->PushBack((Datum)vsrc->AtInternal(index));
                    imask = blsr(imask);
                }
            }
        }
        *datapos += FilterCalcSize;
    }

    return;
}

template <uint32 unit>
void ColumnVectorFixLen<unit>::VectorOnePack(CVector* src, uint64 mask, int32* datapos) {
    ColumnVectorFixLen<unit>* vsrc = static_cast<ColumnVectorFixLen<unit>*>(src);
    uint64 imask = mask;
    size_t index;
    
    while (imask) {
        index = *datapos + __builtin_ctzll(imask);
        this->PushBack((Datum)vsrc->AtInternal(index));
        imask = blsr(imask);
    }

    return;
}

template <uint32 unit>
void ColumnVectorFixLen<unit>::VectorPack(CVector* src, VecQualResult* QualResult, int32* datapos) {
    ColumnVectorFixLen<unit>* vsrc = static_cast<ColumnVectorFixLen<unit>*>(src);
    uint32 count, left_count, leftrows, index;
    int32 pos;
    uint64 mask;

    if (unlikely(_len == ColumnVectorSize))
        return;

    count = *datapos / FilterCalcSize;
    left_count = *datapos & (FilterCalcSize - 1);
    pos = count * FilterCalcSize;
    Assert(count <= QualResult->count);

    if (left_count)
        mask = (((uint64)-1) << left_count) & QualResult->masks[count];
    else
        mask = QualResult->masks[count];

    if (unlikely(count == QualResult->count))
        goto leftpack;

    while (true) {
        if (unlikely(_len + __builtin_popcountll(mask) > ColumnVectorSize)) {
            leftrows = ColumnVectorSize - _len;
            index = pos - 1;
            while (leftrows) {
                index = pos + __builtin_ctzll(mask);
                this->PushBack((Datum)vsrc->AtInternal(index));
                mask = blsr(mask);
                leftrows--;
            }
            *datapos += index - pos + 1;
            return;
        }

        const uint8_t prefix_to_copy = prefixToCopy(mask);
        if (0xFF != prefix_to_copy) {
            this->BatchPushBack(src, pos, pos + prefix_to_copy);
        }
        else {
            const uint8_t suffix_to_copy = suffixToCopy(mask);
            if (0xFF != suffix_to_copy) {
                this->BatchPushBack(src, pos + FilterCalcSize - suffix_to_copy, pos + FilterCalcSize);
            }
            else {
                while (mask) {
                    index = pos + __builtin_ctzll(mask);
                    this->PushBack((Datum)vsrc->AtInternal(index));
                    mask = blsr(mask);
                }
            }
        }

        *datapos += FilterCalcSize - left_count;
        left_count = 0;
        pos += FilterCalcSize; 
        ++count;
        Assert(*datapos % FilterCalcSize == 0);

        if (count == QualResult->count)
            break;
        mask = QualResult->masks[count]; 
    }
    mask = QualResult->masks[count];

leftpack:
    if (unlikely(QualResult->left_count)) {
        if (unlikely(_len + __builtin_popcountll(mask) > ColumnVectorSize)) {
            leftrows = ColumnVectorSize - _len;
            index = pos - 1;
            while (leftrows) {
                index = pos + __builtin_ctzll(mask);
                this->PushBack((Datum)vsrc->AtInternal(index));
                mask = blsr(mask);
                leftrows--;
            }
            *datapos += index - pos + 1;
        }
        else {
            while (mask) {
                index = pos + __builtin_ctzll(mask);
                this->PushBack((Datum)vsrc->AtInternal(index));
                mask = blsr(mask);
            }
            *datapos += QualResult->left_count - left_count;
        }
    }

    return;
}

template <uint32 unit>
bool ColumnVectorFixLen<unit>::AtSame(int idx, void* value, uint32 size) {
    return !memcmp(this->AtInternal(idx), value, unit);
}

/* ColumnVectorStr */

void ColumnVectorStr::Init() {
    Assert(_bitmap == NULL);
    Assert(_offset == NULL);
    _len = 0;
    _nulllen = 0;
    _bitmap = (uint8*)MemoryContextAlloc(_cxt, sizeof(uint8) * ColumnVectorSize);
    _data.Alloc(_cxt, ColumnVectorStrDefaultSize);
    _offset = (uint32*)CacheLineAlign(MemoryContextAlloc(_cxt, sizeof(uint32) * (ColumnVectorSize + 1) + CacheLineSize));
    _offset[0] = 0;
    _offset += 1;
}

void ColumnVectorStr::InitByAddr(void* ptr, uint32 len, uint32 nulllen, uint32* offset, uint32 size, uint8* bitmap) {   
    Assert(ptr);
    Assert(offset);
    Assert(nulllen == 0 || (nulllen > 0 && bitmap));
    Assert(offset[-1] == 0);

    _len = len; 
    _nulllen = nulllen;
    _bitmap = bitmap;
    _data.SetData((DT*)ptr, size);
    _offset = offset;
}

void ColumnVectorStr::InitByValue(const void* data, uint32 len, uint32 size) {
    Assert(data);
    Assert(len);
    Assert(size);
    _len = len;
    _nulllen = 0;
    _bitmap = NULL;

    uint32 offset = 0;
    for (uint32 i = 0; i < _len; ++i) {
        memcpy(_data.Start() + (i * offset), data, size);  
        offset += size;
        _offset[i] = offset;
    }
}

void ColumnVectorStr::PushBack(Datum value, uint32 size) {
    DT* v = reinterpret_cast<DT*>(value);
    
    if (unlikely(this->_data.NoEnough(size)))
        do { this->_data.Realloc(); } while (unlikely(this->_data.NoEnough(size)));

    this->_data.PushBack(v, size);
    this->_offset[_len] = this->_offset[static_cast<int>(_len) - 1] + size;
    ++_len;

    return;
}

void ColumnVectorStr::BatchPushBack(CVector* src, int32 start, int32 end) {
    ColumnVectorStr* v = static_cast<ColumnVectorStr*>(src);
    uint32 size;
    uint32 len;
    int32 base;

    len = end - start;
    if (likely(len)) {
        size = v->_offset[end - 1] - v->_offset[start - 1];
        if (unlikely(this->_data.NoEnough(size)))
            do { this->_data.Realloc(); } while (unlikely(this->_data.NoEnough(size)));

        this->_data.PushBack(v->AtInternal(start), size);
        base = this->_offset[static_cast<int32>(_len) - 1] - v->_offset[start - 1];

        for (int32 i = start; i < end; ++i) {
            this->_offset[_len++] = static_cast<int32>(v->_offset[i]) + base;
        }

        //if (len == FilterCalcSize) {
        //    __m128i b = _mm_set_epi32(base, base, base, base);
        //    for (int32 i = start; i < end; i += 4) {
        //        __m128i o = _mm_loadu_si128(reinterpret_cast<const __m128i*>(&(v->_offset[i])));
        //        __m128i a = _mm_add_epi32(o, b);
        //        _mm_store_si128(reinterpret_cast<__m128i*>(&(this->_offset[_len])), a);
        //        _len += 4;
        //    }
        //}
    }

    return;
}

void ColumnVectorStr::ShallowCopy(CVector* vector) {
    ColumnVectorStr *v = static_cast<ColumnVectorStr*>(vector);
    Assert(_data.IsUnalloc());
    _len = v->_len;
    _nulllen = v->_nulllen;
    _bitmap = v->_bitmap;
    _data.ShallowCopy(&(v->_data));
    _offset = v->_offset;
}

void ColumnVectorStr::DeepCopy(CVector* vector) {
    ColumnVectorStr* v = static_cast<ColumnVectorStr*>(vector);
    uint32 size;

    _len = v->_len;
    _nulllen = v->_nulllen;
    if (_nulllen) {
        Assert(_bitmap);
        memcpy(_bitmap, v->_bitmap, _len * sizeof(uint8));
    }

    size = v->_offset[_len - 1];
    if (unlikely(this->_data.NoEnough(size)))
        do { this->_data.Realloc(); } while (unlikely(this->_data.NoEnough(size)));

    this->_data.PushBack(v->_data.Start(), size);
    memcpy(_offset, v->_offset, _len * sizeof(uint32));
}

void ColumnVectorStr::VectorBatchPack(CVector* src, uint64* masks, uint32 count, int32* datapos) {
    ColumnVectorStr* vsrc = static_cast<ColumnVectorStr*>(src);
    
    for (uint32 i = 0;i < count; ++i) {
        const uint8_t prefix_to_copy = prefixToCopy(masks[i]);
        if (0xFF != prefix_to_copy) {
            this->BatchPushBack(src, *datapos, *datapos + prefix_to_copy);
        }
        else {
            const uint8_t suffix_to_copy = suffixToCopy(masks[i]);
            if (0xFF != suffix_to_copy) {
                this->BatchPushBack(src, *datapos + FilterCalcSize - suffix_to_copy, *datapos + FilterCalcSize);
            }
            else {
                uint64 imask = masks[i];
                size_t index;
                while (imask) {
                    index = *datapos + __builtin_ctzll(imask);
                    this->PushBack((Datum)vsrc->AtInternal(index), vsrc->AtSize(index));
                    imask = blsr(imask);
                }
            }
        }
        *datapos += FilterCalcSize;
    }

    return;
}

void ColumnVectorStr::VectorOnePack(CVector* src, uint64 mask, int32* datapos) {
    ColumnVectorStr* vsrc = static_cast<ColumnVectorStr*>(src);
    uint64 imask = mask;
    size_t index;
    
    while (imask) {
        index = *datapos + __builtin_ctzll(imask);
        this->PushBack((Datum)vsrc->AtInternal(index), vsrc->AtSize(index));
        imask = blsr(imask);
    }

    return;
}

void ColumnVectorStr::PrefetchVectorData() {
    uint32 count = _data.Size() / CacheLineSize;
    uint32 offsetcount = (_len * sizeof(uint32)) / CacheLineSize; 
    uint32 offset = 0;

    for (uint32 i = 0;i < count; ++i) {
        __builtin_prefetch(_data.Start() + offset, 0, 2);
        offset += CacheLineSize;
    }

    for (uint32 i = 0;i < offsetcount; ++i) {
        __builtin_prefetch(_offset + offset, 0, 2);
        offset += CacheLineSize;
    }
}

void ColumnVectorStr::VectorPack(CVector* src, VecQualResult* QualResult, int32* datapos) {
    ColumnVectorStr* vsrc = static_cast<ColumnVectorStr*>(src);
    uint32 count, left_count, leftrows, index;
    int32 pos;
    uint64 mask;

    if (unlikely(_len == ColumnVectorSize))
        return;

    count = *datapos / FilterCalcSize;
    left_count = *datapos & (FilterCalcSize - 1);
    pos = count * FilterCalcSize;
    Assert(count <= QualResult->count);

    if (left_count)
        mask = (((uint64)-1) << left_count) & QualResult->masks[count];
    else
        mask = QualResult->masks[count];

    if (unlikely(count == QualResult->count))
        goto leftpack;

    while (true) {
        if (unlikely(_len + __builtin_popcountll(mask) > ColumnVectorSize)) {
            leftrows = ColumnVectorSize - _len;
            index = pos - 1;
            while (leftrows) {
                index = pos + __builtin_ctzll(mask);
                this->PushBack((Datum)vsrc->AtInternal(index), vsrc->AtSize(index));
                mask = blsr(mask);
                leftrows--;
            }
            *datapos += index - pos + 1;
            return;
        }

        const uint8_t prefix_to_copy = prefixToCopy(mask);
        if (0xFF != prefix_to_copy) {
            this->BatchPushBack(src, pos, pos + prefix_to_copy);
        }
        else {
            const uint8_t suffix_to_copy = suffixToCopy(mask);
            if (0xFF != suffix_to_copy) {
                this->BatchPushBack(src, pos + FilterCalcSize - suffix_to_copy, pos + FilterCalcSize);
            }
            else {
                while (mask) {
                    index = pos + __builtin_ctzll(mask);
                    this->PushBack((Datum)vsrc->AtInternal(index), vsrc->AtSize(index));
                    mask = blsr(mask);
                }
            }
        }

        *datapos += FilterCalcSize - left_count;
        left_count = 0;
        pos += FilterCalcSize; 
        ++count;
        Assert(*datapos % FilterCalcSize == 0);

        if (count == QualResult->count)
            break;
        mask = QualResult->masks[count]; 
    }
    mask = QualResult->masks[count];

leftpack:
    if (unlikely(QualResult->left_count)) {
        if (unlikely(_len + __builtin_popcountll(mask) > ColumnVectorSize)) {
            leftrows = ColumnVectorSize - _len;
            index = pos - 1;
            while (leftrows) {
                index = pos + __builtin_ctzll(mask);
                this->PushBack((Datum)vsrc->AtInternal(index), vsrc->AtSize(index));
                mask = blsr(mask);
                leftrows--;
            }
            *datapos += index - pos + 1;
        }
        else {
            while (mask) {
                index = pos + __builtin_ctzll(mask);
                this->PushBack((Datum)vsrc->AtInternal(index), vsrc->AtSize(index));
                mask = blsr(mask);
            }
            *datapos += QualResult->left_count - left_count;
        }
    }

    return;
}

bool ColumnVectorStr::AtSame(int idx, void* value, uint32 size) {
    uint32 len = this->AtSize(idx) > size ? size : this->AtSize(idx);
    return !memcmp(this->AtInternal(idx), value, len);
}

/* ColumnVectorConst */

template <typename T>
void ColumnVectorConst<T>::InitByValue(const void* data, uint32 len, uint32 size) {
    Assert(len);
    _len = len;
    _data = *((T*)data);
    _size = sizeof(T);
}

template <>
void ColumnVectorConst<uint8*>::InitByValue(const void* data, uint32 len, uint32 size) {
    Assert(len);
    Assert(size);
    _len = len;
    _data = (uint8*)MemoryContextAlloc(_cxt, size);
    memcpy(_data, data, size);
    _size = size;
}

template <typename T>
bool ColumnVectorConst<T>::AtSame(int idx, void* value, uint32 size) {
    return *((T*)value) == _data;
}

template <>
bool ColumnVectorConst<uint8*>::AtSame(int idx, void* value, uint32 size) {
    uint32 len = _size > size ? size : _size;
    return !memcmp(_data, value, len);
}

template <typename T>
void ColumnVectorConst<T>::ShallowCopy(CVector* vector) {
    ColumnVectorConst<T> *v = static_cast<ColumnVectorConst<T>*>(vector);
    _len = v->_len;
    _data = v->_data;
}

template <typename T>
void ColumnVectorConst<T>::DeepCopy(CVector* vector) {
    ColumnVectorConst<T> *v = static_cast<ColumnVectorConst<T>*>(vector);
    _len = v->_len;
    _data = v->_data;
}

/* BatchVector */

BatchVector::BatchVector(MemoryContext cxt, TupleDesc desc, Bitmapset* AssignColumn, bool NeedAlign) {
    FormData_pg_attribute* attrs = desc->attrs;
    uint32 alloc_size = 0;
    unsigned char* saddr = NULL;
    _cols = desc->natts;
    _rows = 0;

    for (uint32 i = 0;i < _cols;++i) {
        if (AssignColumn == NULL || (AssignColumn && bms_is_member(i, AssignColumn))) {
            switch (attrs[i].atttypid) {
                case INT1OID:
                    alloc_size += sizeof(ColumnVectorInt8);
                    break;
                case INT2OID:
                    alloc_size += sizeof(ColumnVectorInt16);
                    break;
                case INT4OID:
                case DATEOID:
                    alloc_size += sizeof(ColumnVectorInt32);
                    break;
                case INT8OID:
                case TIMESTAMPOID:
                case TIDOID:
                    alloc_size += sizeof(ColumnVectorInt64);
                    break;
                case FLOAT4OID:
                case FLOAT4ARRAYOID:
                    alloc_size += sizeof(ColumnVectorFloat4);
                    break;
                case FLOAT8OID:
                case FLOAT8ARRAYOID:
                    alloc_size += sizeof(ColumnVectorFloat8);
                    break;
                case TEXTOID:
                case BPCHAROID:
                case VARCHAROID:
                    alloc_size += sizeof(ColumnVectorStr);
                    break;
                case MACADDROID:
                    alloc_size += sizeof(ColumnVectorMacAddr);
                    break;
                case TIMETZOID:
                    alloc_size += sizeof(ColumnVectorTimeZone);
                    break;
                case TINTERVALOID:
                    alloc_size += sizeof(ColumnVectorTInterval);
                    break;
                case INTERVALOID:
                    alloc_size += sizeof(ColumnVectorInterval);
                    break;                
                case UUIDOID:
                    alloc_size += sizeof(ColumnVectorUUID);
                    break;
                case NAMEOID:
                    alloc_size += sizeof(ColumnVectorName);
                    break;
                default:
                    elog(ERROR, "should add new type: %d", attrs[i].atttypid);
                    break;
            }
        }
    }

    MemoryContext oldcxt = MemoryContextSwitchTo(cxt);
    if (NeedAlign)
        saddr = (unsigned char*)CacheLineAlign(palloc(sizeof(CVector*) * _cols + alloc_size + CacheLineSize));
    else
        saddr = (unsigned char*)palloc(sizeof(CVector*) * _cols + alloc_size);
    MemoryContextSwitchTo(oldcxt);

    _vectors = (CVector**)(saddr);
    saddr += sizeof(CVector*) * _cols;
    
    for (uint32 i = 0;i < _cols;++i) {
        if (AssignColumn && !bms_is_member(i, AssignColumn)) {
            _vectors[i] = NULL;
            continue;
        }

        switch (attrs[i].atttypid) {
            case INT1OID:
                _vectors[i] = new(saddr) ColumnVectorInt8(cxt);
                saddr += sizeof(ColumnVectorInt8);
                break;
            case INT2OID:
                _vectors[i] = new(saddr) ColumnVectorInt16(cxt);
                saddr += sizeof(ColumnVectorInt16);
                break;
            case INT4OID:
            case DATEOID:
                _vectors[i] = new(saddr) ColumnVectorInt32(cxt);
                saddr += sizeof(ColumnVectorInt32);
                break;
            case INT8OID:
            case TIMESTAMPOID:
            case TIDOID:
                _vectors[i] = new(saddr) ColumnVectorInt64(cxt);
                saddr += sizeof(ColumnVectorInt64);
                break;
            case FLOAT4OID:
            case FLOAT4ARRAYOID:
                _vectors[i] = new(saddr) ColumnVectorFloat4(cxt);
                saddr += sizeof(ColumnVectorFloat4);
                break;
            case FLOAT8OID:
            case FLOAT8ARRAYOID:
                _vectors[i] = new(saddr) ColumnVectorFloat8(cxt);
                saddr += sizeof(ColumnVectorFloat8);
                break;
            case TEXTOID:
            case BPCHAROID:
            case VARCHAROID:
                _vectors[i] = new(saddr) ColumnVectorStr(cxt);
                saddr += sizeof(ColumnVectorStr);
                break;
            case MACADDROID:
                _vectors[i] = new(saddr) ColumnVectorMacAddr(cxt);
                alloc_size += sizeof(ColumnVectorMacAddr);
                break;
            case TIMETZOID:
                _vectors[i] = new(saddr) ColumnVectorTimeZone(cxt);
                alloc_size += sizeof(ColumnVectorTimeZone);
                break;
            case TINTERVALOID:
                _vectors[i] = new(saddr) ColumnVectorTInterval(cxt);
                alloc_size += sizeof(ColumnVectorTInterval);
                break;
            case INTERVALOID:
                _vectors[i] = new(saddr) ColumnVectorInterval(cxt);
                alloc_size += sizeof(ColumnVectorInterval);
                break;                
            case UUIDOID:
                _vectors[i] = new(saddr) ColumnVectorUUID(cxt);
                alloc_size += sizeof(ColumnVectorUUID);
                break;
            case NAMEOID:
                _vectors[i] = new(saddr) ColumnVectorName(cxt);
                alloc_size += sizeof(ColumnVectorName);
                break;
            default:
                break;
        }
    }
}

CVector* AllocColumnVectorByType(MemoryContext cxt, int typeId) 
{
    switch (typeId) {
        case INT1OID:
            return (CVector*)New(cxt) ColumnVectorInt8(cxt);
        case BOOLOID:
            return (CVector*)New(cxt) ColumnVectorUint8(cxt);
        case INT2OID:
            return (CVector*)New(cxt) ColumnVectorInt16(cxt);
        case INT4OID:
        case DATEOID:
            return (CVector*)New(cxt) ColumnVectorInt32(cxt);
        case INT8OID:
        case TIMESTAMPOID:
        case TIDOID:
            return (CVector*)New(cxt) ColumnVectorInt64(cxt);
        case FLOAT4OID:
        case FLOAT4ARRAYOID:
            return (CVector*)New(cxt) ColumnVectorFloat4(cxt);
        case FLOAT8OID:
        case FLOAT8ARRAYOID:
            return (CVector*)New(cxt) ColumnVectorFloat8(cxt);
        case TEXTOID:
        case BPCHAROID:
        case VARCHAROID:
            return (CVector*)New(cxt) ColumnVectorStr(cxt);
        case MACADDROID:
            return (CVector*)New(cxt) ColumnVectorMacAddr(cxt);
        case TIMETZOID:
            return (CVector*)New(cxt) ColumnVectorTimeZone(cxt);
        case TINTERVALOID:
            return (CVector*)New(cxt) ColumnVectorTInterval(cxt);
        case INTERVALOID:
            return (CVector*)New(cxt) ColumnVectorInterval(cxt);          
        case UUIDOID:
            return (CVector*)New(cxt) ColumnVectorUUID(cxt);
        case NAMEOID:
            return (CVector*)New(cxt) ColumnVectorName(cxt);
        default:
            elog(ERROR, "should add new type: %d", typeId);
            break;
    }

    return NULL;
}

CVector* CreateColumnVectorConst(MemoryContext cxt, Const *con)
{
    switch (con->consttype) {
        case INT1OID: {
            ColumnVectorConstInt8* vector = (ColumnVectorConstInt8*)New(cxt) ColumnVectorConstInt8(cxt);
            vector->InitByValue(&con->constvalue, ColumnVectorSize);
            return vector;
        }
        case INT2OID: {
            ColumnVectorConstInt16* vector = (ColumnVectorConstInt16*)New(cxt) ColumnVectorConstInt16(cxt);
            vector->InitByValue(&con->constvalue, ColumnVectorSize);
            return vector;
        }
        case INT4OID: {
            ColumnVectorConstInt32* vector = (ColumnVectorConstInt32*)New(cxt) ColumnVectorConstInt32(cxt);
            vector->InitByValue(&con->constvalue, ColumnVectorSize);
            return vector;
        }
        case INT8OID:
        case TIMESTAMPOID:
        case TIDOID: {
            ColumnVectorConstInt64* vector = (ColumnVectorConstInt64*)New(cxt) ColumnVectorConstInt64(cxt);
            vector->InitByValue(&con->constvalue, ColumnVectorSize);
            return vector;
        }
        case FLOAT4OID: {
            ColumnVectorConstFloat4* vector = (ColumnVectorConstFloat4*)New(cxt) ColumnVectorConstFloat4(cxt);
            vector->InitByValue(&con->constvalue, ColumnVectorSize);
            return vector;
        }
        case FLOAT8OID: {
            ColumnVectorConstFloat8* vector = (ColumnVectorConstFloat8*)New(cxt) ColumnVectorConstFloat8(cxt);
            vector->InitByValue(&con->constvalue, ColumnVectorSize);
            return vector;
        }
        case TEXTOID: 
        case BPCHAROID:
        case VARCHAROID: { 
            ColumnVectorConstStr* vector = (ColumnVectorConstStr*)New(cxt) ColumnVectorConstStr(cxt);
            vector->InitByValue(VARDATA_ANY(con->constvalue), ColumnVectorSize, VARSIZE_ANY(con->constvalue) - VARHDRSZ);
            return vector;
        }
        default:
            elog(ERROR, "should add new type: %d", con->consttype);
            break;
    }

    return NULL;
}

uint64 bytes64MaskToBits64Mask(const uint8 * bytes64)
{
#if defined(_M_AMD64) || defined(_M_X64) || defined(__amd64) || defined(__x86_64__)
    const __m128i zero16 = _mm_setzero_si128();
    uint64 res =
        (static_cast<uint64>(_mm_movemask_epi8(_mm_cmpeq_epi8(
        _mm_loadu_si128(reinterpret_cast<const __m128i *>(bytes64)), zero16))) & 0xffff)
        | ((static_cast<uint64>(_mm_movemask_epi8(_mm_cmpeq_epi8(
        _mm_loadu_si128(reinterpret_cast<const __m128i *>(bytes64 + 16)), zero16))) << 16) & 0xffff0000)
        | ((static_cast<uint64>(_mm_movemask_epi8(_mm_cmpeq_epi8(
        _mm_loadu_si128(reinterpret_cast<const __m128i *>(bytes64 + 32)), zero16))) << 32) & 0xffff00000000)
        | ((static_cast<uint64>(_mm_movemask_epi8(_mm_cmpeq_epi8(
        _mm_loadu_si128(reinterpret_cast<const __m128i *>(bytes64 + 48)), zero16))) << 48) & 0xffff000000000000);
#elif defined(_M_ARM64) || defined(__aarch64__)
    const uint8x16_t bitmask = {0x01, 0x02, 0x4, 0x8, 0x10, 0x20, 0x40, 0x80, 0x01, 0x02, 0x4, 0x8, 0x10, 0x20, 0x40, 0x80};
    const auto * src = reinterpret_cast<const unsigned char *>(bytes64);
    const uint8x16_t p0 = vceqzq_u8(vld1q_u8(src));
    const uint8x16_t p1 = vceqzq_u8(vld1q_u8(src + 16));
    const uint8x16_t p2 = vceqzq_u8(vld1q_u8(src + 32));
    const uint8x16_t p3 = vceqzq_u8(vld1q_u8(src + 48));
    uint8x16_t t0 = vandq_u8(p0, bitmask);
    uint8x16_t t1 = vandq_u8(p1, bitmask);
    uint8x16_t t2 = vandq_u8(p2, bitmask);
    uint8x16_t t3 = vandq_u8(p3, bitmask);
    uint8x16_t sum0 = vpaddq_u8(t0, t1);
    uint8x16_t sum1 = vpaddq_u8(t2, t3);
    sum0 = vpaddq_u8(sum0, sum1);
    sum0 = vpaddq_u8(sum0, sum0);
    uint64 res = vgetq_lane_u64(vreinterpretq_u64_u8(sum0), 0);
#else
    uint64 res = 0;
    for (size_t i = 0; i < 64; ++i)
        res |= static_cast<uint64>(0 == bytes64[i]) << i;
#endif
    return ~res;
}

inline static uint8 prefixToCopy(uint64 mask)
{
    if (mask == 0)
        return 0;
    if (mask == static_cast<uint64>(-1))
        return FilterCalcSize;
    const uint64 leading_zeroes = __builtin_clzll(mask);
    if (mask == ((static_cast<uint64>(-1) << leading_zeroes) >> leading_zeroes))
        return FilterCalcSize - leading_zeroes;
    else
        return 0xFF;
}

inline static uint8 suffixToCopy(uint64 mask)
{
    const auto prefix_to_copy = prefixToCopy(~mask);
    return prefix_to_copy >= FilterCalcSize ? prefix_to_copy : FilterCalcSize - prefix_to_copy;
}
