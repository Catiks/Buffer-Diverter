-- b compatibility case
drop database if exists b;
create database b dbcompatibility 'b';

select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef');
select aes_decrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef');

\c b
-- aes_encrypt(str, key_str, init_vector)
set block_encryption_mode = 'aes-128-cbc';
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', 'abcd');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef');
select lengthb(aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef123456')), lengthb('HuaweiGauss_234');
select lengthb(aes_encrypt('1234567890', 'cdjsfj3713vdVHV1', '1234567890abcdef123456')), lengthb('1234567890');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef') = aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef123456');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890dgjahdjk');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef') = aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890dgjahdjk');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', NULL);
select aes_encrypt('HuaweiGauss_234', NULL, '1234567890dgjahdjk');
select aes_encrypt(NULL, 'cdjsfj3713vdVHV1', '1234567890dgjahdjk');
select aes_encrypt(NULL, NULL, '1234567890dgjahdjk');
select aes_encrypt(NULL, 'dadsads'), aes_encrypt('dadsadsadsadfcs', NULL), aes_encrypt(NULL, NULL);
select aes_encrypt(NULL, 'dadsads', 'dadsadsadsa'), aes_encrypt('dadsadsadsadfcs', NULL, 'dadsadsadsa'), aes_encrypt(NULL, NULL, NULL);

select aes_decrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1');
select aes_decrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', 'abcd');
create table test_encrypt(f1 text);
insert into test_encrypt select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef');
select aes_decrypt(f1, 'cdjsfj3713vdVHV1', '1234567890abcdef') from test_encrypt;
select aes_decrypt(f1, 'cdjsfj3713vdVHV1', '1234567890abcdef') = 'HuaweiGauss_234' from test_encrypt;
select aes_decrypt(aes_encrypt('~`!@#$%^&*()_+-=<>?,./;''“”""[]{}\|','cdjsfj3713vdVHV1', '1234567890abcdef'),'cdjsfj3713vdVHV1', '1234567890abcdef');
select aes_decrypt(NULL, NULL, 'dadsa'), aes_decrypt(NULL, 'dasdsa', 'dsadsa'), aes_decrypt('dadsad', NULL, 'dadsadsa');
select aes_decrypt(aes_encrypt('大家好','cdjsfj3713vdVHV1', '1234567890abcdef'),'cdjsfj3713vdVHV1', '1234567890abcdef');
select aes_decrypt(aes_encrypt(E'd\nsa\tdj\bsaf\Z\\dssa\ca\rs','dajd123FGBJG', '1234567890abcdef'),'dajd123FGBJG', '1234567890abcdef');
select aes_decrypt(aes_encrypt('~·！@#￥%……&*（）-=——+{}【】、|：‘’，。《》、？','12345dvghadCVBUJNF', '1234567890abcdef'),'12345dvghadCVBUJNF', '1234567890abcdef');
drop table test_encrypt;

set block_encryption_mode = 'aes-192-cbc';
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', 'abcd');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef123456');
select lengthb(aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef123456')), lengthb('HuaweiGauss_234');
select lengthb(aes_encrypt('1234567890', 'cdjsfj3713vdVHV1', '1234567890abcdef123456')), lengthb('1234567890');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef') = aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef123456');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890dgjahdjk');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef') = aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890dgjahdjk');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', NULL);
select aes_encrypt('HuaweiGauss_234', NULL, '1234567890dgjahdjk');
select aes_encrypt(NULL, 'cdjsfj3713vdVHV1', '1234567890dgjahdjk');
select aes_encrypt(NULL, NULL, '1234567890dgjahdjk');
select aes_encrypt(NULL, 'dadsads'), aes_encrypt('dadsadsadsadfcs', NULL), aes_encrypt(NULL, NULL);
select aes_encrypt(NULL, 'dadsads', 'dadsadsadsa'), aes_encrypt('dadsadsadsadfcs', NULL, 'dadsadsadsa'), aes_encrypt(NULL, NULL, NULL);

select aes_decrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1');
select aes_decrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', 'abcd');
create table test_encrypt(f1 text);
insert into test_encrypt select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef');
select aes_decrypt(f1, 'cdjsfj3713vdVHV1', '1234567890abcdef') from test_encrypt;
select aes_decrypt(f1, 'cdjsfj3713vdVHV1', '1234567890abcdef') = 'HuaweiGauss_234' from test_encrypt;
select aes_decrypt(aes_encrypt('~`!@#$%^&*()_+-=<>?,./;''“”""[]{}\|','cdjsfj3713vdVHV1', '1234567890abcdef'),'cdjsfj3713vdVHV1', '1234567890abcdef');
select aes_decrypt(NULL, NULL, 'dadsa'), aes_decrypt(NULL, 'dasdsa', 'dsadsa'), aes_decrypt('dadsad', NULL, 'dadsadsa');
select aes_decrypt(aes_encrypt('大家好','cdjsfj3713vdVHV1', '1234567890abcdef'),'cdjsfj3713vdVHV1', '1234567890abcdef');
select aes_decrypt(aes_encrypt(E'd\nsa\tdj\bsaf\Z\\dssa\ca\rs','dajd123FGBJG', '1234567890abcdef'),'dajd123FGBJG', '1234567890abcdef');
select aes_decrypt(aes_encrypt('~·！@#￥%……&*（）-=——+{}【】、|：‘’，。《》、？','12345dvghadCVBUJNF', '1234567890abcdef'),'12345dvghadCVBUJNF', '1234567890abcdef');
drop table test_encrypt;

set block_encryption_mode = 'aes-256-cbc';
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', 'abcd');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef123456');
select lengthb(aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef123456')), lengthb('HuaweiGauss_234');
select lengthb(aes_encrypt('1234567890', 'cdjsfj3713vdVHV1', '1234567890abcdef123456')), lengthb('1234567890');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef') = aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef123456');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890dgjahdjk');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef') = aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890dgjahdjk');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', NULL);
select aes_encrypt('HuaweiGauss_234', NULL, '1234567890dgjahdjk');
select aes_encrypt(NULL, 'cdjsfj3713vdVHV1', '1234567890dgjahdjk');
select aes_encrypt(NULL, NULL, '1234567890dgjahdjk');
select aes_encrypt(NULL, 'dadsads'), aes_encrypt('dadsadsadsadfcs', NULL), aes_encrypt(NULL, NULL);
select aes_encrypt(NULL, 'dadsads', 'dadsadsadsa'), aes_encrypt('dadsadsadsadfcs', NULL, 'dadsadsadsa'), aes_encrypt(NULL, NULL, NULL);

select aes_decrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1');
select aes_decrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', 'abcd');
create table test_encrypt(f1 text);
insert into test_encrypt select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef');
select aes_decrypt(f1, 'cdjsfj3713vdVHV1', '1234567890abcdef') from test_encrypt;
select aes_decrypt(f1, 'cdjsfj3713vdVHV1', '1234567890abcdef') = 'HuaweiGauss_234' from test_encrypt;
select aes_decrypt(aes_encrypt('~`!@#$%^&*()_+-=<>?,./;''“”""[]{}\|','cdjsfj3713vdVHV1', '1234567890abcdef'),'cdjsfj3713vdVHV1', '1234567890abcdef');
select aes_decrypt(NULL, NULL, 'dadsa'), aes_decrypt(NULL, 'dasdsa', 'dsadsa'), aes_decrypt('dadsad', NULL, 'dadsadsa');
select aes_decrypt(aes_encrypt('大家好','cdjsfj3713vdVHV1', '1234567890abcdef'),'cdjsfj3713vdVHV1', '1234567890abcdef');
select aes_decrypt(aes_encrypt(E'd\nsa\tdj\bsaf\Z\\dssa\ca\rs','dajd123FGBJG', '1234567890abcdef'),'dajd123FGBJG', '1234567890abcdef');
select aes_decrypt(aes_encrypt('~·！@#￥%……&*（）-=——+{}【】、|：‘’，。《》、？','12345dvghadCVBUJNF', '1234567890abcdef'),'12345dvghadCVBUJNF', '1234567890abcdef');
drop table test_encrypt;

set block_encryption_mode = 'aes-128-cfb1';
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', 'abcd');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef123456');
select lengthb(aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef123456')), lengthb('HuaweiGauss_234');
select lengthb(aes_encrypt('1234567890', 'cdjsfj3713vdVHV1', '1234567890abcdef123456')), lengthb('1234567890');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef') = aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef123456');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890dgjahdjk');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef') = aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890dgjahdjk');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', NULL);
select aes_encrypt('HuaweiGauss_234', NULL, '1234567890dgjahdjk');
select aes_encrypt(NULL, 'cdjsfj3713vdVHV1', '1234567890dgjahdjk');
select aes_encrypt(NULL, NULL, '1234567890dgjahdjk');
select aes_encrypt(NULL, 'dadsads'), aes_encrypt('dadsadsadsadfcs', NULL), aes_encrypt(NULL, NULL);
select aes_encrypt(NULL, 'dadsads', 'dadsadsadsa'), aes_encrypt('dadsadsadsadfcs', NULL, 'dadsadsadsa'), aes_encrypt(NULL, NULL, NULL);

select aes_decrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1');
select aes_decrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', 'abcd');
create table test_encrypt(f1 text);
insert into test_encrypt select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef');
select aes_decrypt(f1, 'cdjsfj3713vdVHV1', '1234567890abcdef') from test_encrypt;
select aes_decrypt(f1, 'cdjsfj3713vdVHV1', '1234567890abcdef') = 'HuaweiGauss_234' from test_encrypt;
select aes_decrypt(aes_encrypt('~`!@#$%^&*()_+-=<>?,./;''“”""[]{}\|','cdjsfj3713vdVHV1', '1234567890abcdef'),'cdjsfj3713vdVHV1', '1234567890abcdef');
select aes_decrypt(NULL, NULL, 'dadsa'), aes_decrypt(NULL, 'dasdsa', 'dsadsa'), aes_decrypt('dadsad', NULL, 'dadsadsa');
select aes_decrypt(aes_encrypt('大家好','cdjsfj3713vdVHV1', '1234567890abcdef'),'cdjsfj3713vdVHV1', '1234567890abcdef');
select aes_decrypt(aes_encrypt(E'd\nsa\tdj\bsaf\Z\\dssa\ca\rs','dajd123FGBJG', '1234567890abcdef'),'dajd123FGBJG', '1234567890abcdef');
select aes_decrypt(aes_encrypt('~·！@#￥%……&*（）-=——+{}【】、|：‘’，。《》、？','12345dvghadCVBUJNF', '1234567890abcdef'),'12345dvghadCVBUJNF', '1234567890abcdef');
drop table test_encrypt;

set block_encryption_mode = 'aes-192-cfb1';
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', 'abcd');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef123456');
select lengthb(aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef123456')), lengthb('HuaweiGauss_234');
select lengthb(aes_encrypt('1234567890', 'cdjsfj3713vdVHV1', '1234567890abcdef123456')), lengthb('1234567890');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef') = aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef123456');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890dgjahdjk');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef') = aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890dgjahdjk');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', NULL);
select aes_encrypt('HuaweiGauss_234', NULL, '1234567890dgjahdjk');
select aes_encrypt(NULL, 'cdjsfj3713vdVHV1', '1234567890dgjahdjk');
select aes_encrypt(NULL, NULL, '1234567890dgjahdjk');
select aes_encrypt(NULL, 'dadsads'), aes_encrypt('dadsadsadsadfcs', NULL), aes_encrypt(NULL, NULL);
select aes_encrypt(NULL, 'dadsads', 'dadsadsadsa'), aes_encrypt('dadsadsadsadfcs', NULL, 'dadsadsadsa'), aes_encrypt(NULL, NULL, NULL);

select aes_decrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1');
select aes_decrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', 'abcd');
create table test_encrypt(f1 text);
insert into test_encrypt select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef');
select aes_decrypt(f1, 'cdjsfj3713vdVHV1', '1234567890abcdef') from test_encrypt;
select aes_decrypt(f1, 'cdjsfj3713vdVHV1', '1234567890abcdef') = 'HuaweiGauss_234' from test_encrypt;
select aes_decrypt(aes_encrypt('~`!@#$%^&*()_+-=<>?,./;''“”""[]{}\|','cdjsfj3713vdVHV1', '1234567890abcdef'),'cdjsfj3713vdVHV1', '1234567890abcdef');
select aes_decrypt(NULL, NULL, 'dadsa'), aes_decrypt(NULL, 'dasdsa', 'dsadsa'), aes_decrypt('dadsad', NULL, 'dadsadsa');
select aes_decrypt(aes_encrypt('大家好','cdjsfj3713vdVHV1', '1234567890abcdef'),'cdjsfj3713vdVHV1', '1234567890abcdef');
select aes_decrypt(aes_encrypt(E'd\nsa\tdj\bsaf\Z\\dssa\ca\rs','dajd123FGBJG', '1234567890abcdef'),'dajd123FGBJG', '1234567890abcdef');
select aes_decrypt(aes_encrypt('~·！@#￥%……&*（）-=——+{}【】、|：‘’，。《》、？','12345dvghadCVBUJNF', '1234567890abcdef'),'12345dvghadCVBUJNF', '1234567890abcdef');
drop table test_encrypt;

set block_encryption_mode = 'aes-256-cfb1';
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', 'abcd');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef123456');
select lengthb(aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef123456')), lengthb('HuaweiGauss_234');
select lengthb(aes_encrypt('1234567890', 'cdjsfj3713vdVHV1', '1234567890abcdef123456')), lengthb('1234567890');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef') = aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef123456');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890dgjahdjk');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef') = aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890dgjahdjk');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', NULL);
select aes_encrypt('HuaweiGauss_234', NULL, '1234567890dgjahdjk');
select aes_encrypt(NULL, 'cdjsfj3713vdVHV1', '1234567890dgjahdjk');
select aes_encrypt(NULL, NULL, '1234567890dgjahdjk');
select aes_encrypt(NULL, 'dadsads'), aes_encrypt('dadsadsadsadfcs', NULL), aes_encrypt(NULL, NULL);
select aes_encrypt(NULL, 'dadsads', 'dadsadsadsa'), aes_encrypt('dadsadsadsadfcs', NULL, 'dadsadsadsa'), aes_encrypt(NULL, NULL, NULL);

select aes_decrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1');
select aes_decrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', 'abcd');
create table test_encrypt(f1 text);
insert into test_encrypt select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef');
select aes_decrypt(f1, 'cdjsfj3713vdVHV1', '1234567890abcdef') from test_encrypt;
select aes_decrypt(f1, 'cdjsfj3713vdVHV1', '1234567890abcdef') = 'HuaweiGauss_234' from test_encrypt;
select aes_decrypt(aes_encrypt('~`!@#$%^&*()_+-=<>?,./;''“”""[]{}\|','cdjsfj3713vdVHV1', '1234567890abcdef'),'cdjsfj3713vdVHV1', '1234567890abcdef');
select aes_decrypt(NULL, NULL, 'dadsa'), aes_decrypt(NULL, 'dasdsa', 'dsadsa'), aes_decrypt('dadsad', NULL, 'dadsadsa');
select aes_decrypt(aes_encrypt('大家好','cdjsfj3713vdVHV1', '1234567890abcdef'),'cdjsfj3713vdVHV1', '1234567890abcdef');
select aes_decrypt(aes_encrypt(E'd\nsa\tdj\bsaf\Z\\dssa\ca\rs','dajd123FGBJG', '1234567890abcdef'),'dajd123FGBJG', '1234567890abcdef');
select aes_decrypt(aes_encrypt('~·！@#￥%……&*（）-=——+{}【】、|：‘’，。《》、？','12345dvghadCVBUJNF', '1234567890abcdef'),'12345dvghadCVBUJNF', '1234567890abcdef');
drop table test_encrypt;

set block_encryption_mode = 'aes-128-cfb8';
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', 'abcd');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef123456');
select lengthb(aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef123456')), lengthb('HuaweiGauss_234');
select lengthb(aes_encrypt('1234567890', 'cdjsfj3713vdVHV1', '1234567890abcdef123456')), lengthb('1234567890');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef') = aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef123456');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890dgjahdjk');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef') = aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890dgjahdjk');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', NULL);
select aes_encrypt('HuaweiGauss_234', NULL, '1234567890dgjahdjk');
select aes_encrypt(NULL, 'cdjsfj3713vdVHV1', '1234567890dgjahdjk');
select aes_encrypt(NULL, NULL, '1234567890dgjahdjk');
select aes_encrypt(NULL, 'dadsads'), aes_encrypt('dadsadsadsadfcs', NULL), aes_encrypt(NULL, NULL);
select aes_encrypt(NULL, 'dadsads', 'dadsadsadsa'), aes_encrypt('dadsadsadsadfcs', NULL, 'dadsadsadsa'), aes_encrypt(NULL, NULL, NULL);

select aes_decrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1');
select aes_decrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', 'abcd');
create table test_encrypt(f1 text);
insert into test_encrypt select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef');
select aes_decrypt(f1, 'cdjsfj3713vdVHV1', '1234567890abcdef') from test_encrypt;
select aes_decrypt(f1, 'cdjsfj3713vdVHV1', '1234567890abcdef') = 'HuaweiGauss_234' from test_encrypt;
select aes_decrypt(aes_encrypt('~`!@#$%^&*()_+-=<>?,./;''“”""[]{}\|','cdjsfj3713vdVHV1', '1234567890abcdef'),'cdjsfj3713vdVHV1', '1234567890abcdef');
select aes_decrypt(NULL, NULL, 'dadsa'), aes_decrypt(NULL, 'dasdsa', 'dsadsa'), aes_decrypt('dadsad', NULL, 'dadsadsa');
select aes_decrypt(aes_encrypt('大家好','cdjsfj3713vdVHV1', '1234567890abcdef'),'cdjsfj3713vdVHV1', '1234567890abcdef');
select aes_decrypt(aes_encrypt(E'd\nsa\tdj\bsaf\Z\\dssa\ca\rs','dajd123FGBJG', '1234567890abcdef'),'dajd123FGBJG', '1234567890abcdef');
select aes_decrypt(aes_encrypt('~·！@#￥%……&*（）-=——+{}【】、|：‘’，。《》、？','12345dvghadCVBUJNF', '1234567890abcdef'),'12345dvghadCVBUJNF', '1234567890abcdef');
drop table test_encrypt;

set block_encryption_mode = 'aes-192-cfb8';
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', 'abcd');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef123456');
select lengthb(aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef123456')), lengthb('HuaweiGauss_234');
select lengthb(aes_encrypt('1234567890', 'cdjsfj3713vdVHV1', '1234567890abcdef123456')), lengthb('1234567890');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef') = aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef123456');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890dgjahdjk');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef') = aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890dgjahdjk');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', NULL);
select aes_encrypt('HuaweiGauss_234', NULL, '1234567890dgjahdjk');
select aes_encrypt(NULL, 'cdjsfj3713vdVHV1', '1234567890dgjahdjk');
select aes_encrypt(NULL, NULL, '1234567890dgjahdjk');
select aes_encrypt(NULL, 'dadsads'), aes_encrypt('dadsadsadsadfcs', NULL), aes_encrypt(NULL, NULL);
select aes_encrypt(NULL, 'dadsads', 'dadsadsadsa'), aes_encrypt('dadsadsadsadfcs', NULL, 'dadsadsadsa'), aes_encrypt(NULL, NULL, NULL);

select aes_decrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1');
select aes_decrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', 'abcd');
create table test_encrypt(f1 text);
insert into test_encrypt select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef');
select aes_decrypt(f1, 'cdjsfj3713vdVHV1', '1234567890abcdef') from test_encrypt;
select aes_decrypt(f1, 'cdjsfj3713vdVHV1', '1234567890abcdef') = 'HuaweiGauss_234' from test_encrypt;
select aes_decrypt(aes_encrypt('~`!@#$%^&*()_+-=<>?,./;''“”""[]{}\|','cdjsfj3713vdVHV1', '1234567890abcdef'),'cdjsfj3713vdVHV1', '1234567890abcdef');
select aes_decrypt(NULL, NULL, 'dadsa'), aes_decrypt(NULL, 'dasdsa', 'dsadsa'), aes_decrypt('dadsad', NULL, 'dadsadsa');
select aes_decrypt(aes_encrypt('大家好','cdjsfj3713vdVHV1', '1234567890abcdef'),'cdjsfj3713vdVHV1', '1234567890abcdef');
select aes_decrypt(aes_encrypt(E'd\nsa\tdj\bsaf\Z\\dssa\ca\rs','dajd123FGBJG', '1234567890abcdef'),'dajd123FGBJG', '1234567890abcdef');
select aes_decrypt(aes_encrypt('~·！@#￥%……&*（）-=——+{}【】、|：‘’，。《》、？','12345dvghadCVBUJNF', '1234567890abcdef'),'12345dvghadCVBUJNF', '1234567890abcdef');
drop table test_encrypt;

set block_encryption_mode = 'aes-256-cfb8';
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', 'abcd');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef123456');
select lengthb(aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef123456')), lengthb('HuaweiGauss_234');
select lengthb(aes_encrypt('1234567890', 'cdjsfj3713vdVHV1', '1234567890abcdef123456')), lengthb('1234567890');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef') = aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef123456');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890dgjahdjk');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef') = aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890dgjahdjk');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', NULL);
select aes_encrypt('HuaweiGauss_234', NULL, '1234567890dgjahdjk');
select aes_encrypt(NULL, 'cdjsfj3713vdVHV1', '1234567890dgjahdjk');
select aes_encrypt(NULL, NULL, '1234567890dgjahdjk');
select aes_encrypt(NULL, 'dadsads'), aes_encrypt('dadsadsadsadfcs', NULL), aes_encrypt(NULL, NULL);
select aes_encrypt(NULL, 'dadsads', 'dadsadsadsa'), aes_encrypt('dadsadsadsadfcs', NULL, 'dadsadsadsa'), aes_encrypt(NULL, NULL, NULL);

select aes_decrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1');
select aes_decrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', 'abcd');
create table test_encrypt(f1 text);
insert into test_encrypt select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef');
select aes_decrypt(f1, 'cdjsfj3713vdVHV1', '1234567890abcdef') from test_encrypt;
select aes_decrypt(f1, 'cdjsfj3713vdVHV1', '1234567890abcdef') = 'HuaweiGauss_234' from test_encrypt;
select aes_decrypt(aes_encrypt('~`!@#$%^&*()_+-=<>?,./;''“”""[]{}\|','cdjsfj3713vdVHV1', '1234567890abcdef'),'cdjsfj3713vdVHV1', '1234567890abcdef');
select aes_decrypt(NULL, NULL, 'dadsa'), aes_decrypt(NULL, 'dasdsa', 'dsadsa'), aes_decrypt('dadsad', NULL, 'dadsadsa');
select aes_decrypt(aes_encrypt('大家好','cdjsfj3713vdVHV1', '1234567890abcdef'),'cdjsfj3713vdVHV1', '1234567890abcdef');
select aes_decrypt(aes_encrypt(E'd\nsa\tdj\bsaf\Z\\dssa\ca\rs','dajd123FGBJG', '1234567890abcdef'),'dajd123FGBJG', '1234567890abcdef');
select aes_decrypt(aes_encrypt('~·！@#￥%……&*（）-=——+{}【】、|：‘’，。《》、？','12345dvghadCVBUJNF', '1234567890abcdef'),'12345dvghadCVBUJNF', '1234567890abcdef');
drop table test_encrypt;

set block_encryption_mode = 'aes-128-cfb128';
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', 'abcd');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef123456');
select lengthb(aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef123456')), lengthb('HuaweiGauss_234');
select lengthb(aes_encrypt('1234567890', 'cdjsfj3713vdVHV1', '1234567890abcdef123456')), lengthb('1234567890');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef') = aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef123456');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890dgjahdjk');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef') = aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890dgjahdjk');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', NULL);
select aes_encrypt('HuaweiGauss_234', NULL, '1234567890dgjahdjk');
select aes_encrypt(NULL, 'cdjsfj3713vdVHV1', '1234567890dgjahdjk');
select aes_encrypt(NULL, NULL, '1234567890dgjahdjk');
select aes_encrypt(NULL, 'dadsads'), aes_encrypt('dadsadsadsadfcs', NULL), aes_encrypt(NULL, NULL);
select aes_encrypt(NULL, 'dadsads', 'dadsadsadsa'), aes_encrypt('dadsadsadsadfcs', NULL, 'dadsadsadsa'), aes_encrypt(NULL, NULL, NULL);

select aes_decrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1');
select aes_decrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', 'abcd');
create table test_encrypt(f1 text);
insert into test_encrypt select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef');
select aes_decrypt(f1, 'cdjsfj3713vdVHV1', '1234567890abcdef') from test_encrypt;
select aes_decrypt(f1, 'cdjsfj3713vdVHV1', '1234567890abcdef') = 'HuaweiGauss_234' from test_encrypt;
select aes_decrypt(aes_encrypt('~`!@#$%^&*()_+-=<>?,./;''“”""[]{}\|','cdjsfj3713vdVHV1', '1234567890abcdef'),'cdjsfj3713vdVHV1', '1234567890abcdef');
select aes_decrypt(NULL, NULL, 'dadsa'), aes_decrypt(NULL, 'dasdsa', 'dsadsa'), aes_decrypt('dadsad', NULL, 'dadsadsa');
select aes_decrypt(aes_encrypt('大家好','cdjsfj3713vdVHV1', '1234567890abcdef'),'cdjsfj3713vdVHV1', '1234567890abcdef');
select aes_decrypt(aes_encrypt(E'd\nsa\tdj\bsaf\Z\\dssa\ca\rs','dajd123FGBJG', '1234567890abcdef'),'dajd123FGBJG', '1234567890abcdef');
select aes_decrypt(aes_encrypt('~·！@#￥%……&*（）-=——+{}【】、|：‘’，。《》、？','12345dvghadCVBUJNF', '1234567890abcdef'),'12345dvghadCVBUJNF', '1234567890abcdef');
drop table test_encrypt;

set block_encryption_mode = 'aes-192-cfb128';
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', 'abcd');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef123456');
select lengthb(aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef123456')), lengthb('HuaweiGauss_234');
select lengthb(aes_encrypt('1234567890', 'cdjsfj3713vdVHV1', '1234567890abcdef123456')), lengthb('1234567890');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef') = aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef123456');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890dgjahdjk');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef') = aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890dgjahdjk');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', NULL);
select aes_encrypt('HuaweiGauss_234', NULL, '1234567890dgjahdjk');
select aes_encrypt(NULL, 'cdjsfj3713vdVHV1', '1234567890dgjahdjk');
select aes_encrypt(NULL, NULL, '1234567890dgjahdjk');
select aes_encrypt(NULL, 'dadsads'), aes_encrypt('dadsadsadsadfcs', NULL), aes_encrypt(NULL, NULL);
select aes_encrypt(NULL, 'dadsads', 'dadsadsadsa'), aes_encrypt('dadsadsadsadfcs', NULL, 'dadsadsadsa'), aes_encrypt(NULL, NULL, NULL);

select aes_decrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1');
select aes_decrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', 'abcd');
create table test_encrypt(f1 text);
insert into test_encrypt select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef');
select aes_decrypt(f1, 'cdjsfj3713vdVHV1', '1234567890abcdef') from test_encrypt;
select aes_decrypt(f1, 'cdjsfj3713vdVHV1', '1234567890abcdef') = 'HuaweiGauss_234' from test_encrypt;
select aes_decrypt(aes_encrypt('~`!@#$%^&*()_+-=<>?,./;''“”""[]{}\|','cdjsfj3713vdVHV1', '1234567890abcdef'),'cdjsfj3713vdVHV1', '1234567890abcdef');
select aes_decrypt(NULL, NULL, 'dadsa'), aes_decrypt(NULL, 'dasdsa', 'dsadsa'), aes_decrypt('dadsad', NULL, 'dadsadsa');
select aes_decrypt(aes_encrypt('大家好','cdjsfj3713vdVHV1', '1234567890abcdef'),'cdjsfj3713vdVHV1', '1234567890abcdef');
select aes_decrypt(aes_encrypt(E'd\nsa\tdj\bsaf\Z\\dssa\ca\rs','dajd123FGBJG', '1234567890abcdef'),'dajd123FGBJG', '1234567890abcdef');
select aes_decrypt(aes_encrypt('~·！@#￥%……&*（）-=——+{}【】、|：‘’，。《》、？','12345dvghadCVBUJNF', '1234567890abcdef'),'12345dvghadCVBUJNF', '1234567890abcdef');
drop table test_encrypt;

set block_encryption_mode = 'aes-256-cfb128';
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', 'abcd');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef123456');
select lengthb(aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef123456')), lengthb('HuaweiGauss_234');
select lengthb(aes_encrypt('1234567890', 'cdjsfj3713vdVHV1', '1234567890abcdef123456')), lengthb('1234567890');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef') = aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef123456');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890dgjahdjk');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef') = aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890dgjahdjk');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', NULL);
select aes_encrypt('HuaweiGauss_234', NULL, '1234567890dgjahdjk');
select aes_encrypt(NULL, 'cdjsfj3713vdVHV1', '1234567890dgjahdjk');
select aes_encrypt(NULL, NULL, '1234567890dgjahdjk');
select aes_encrypt(NULL, 'dadsads'), aes_encrypt('dadsadsadsadfcs', NULL), aes_encrypt(NULL, NULL);
select aes_encrypt(NULL, 'dadsads', 'dadsadsadsa'), aes_encrypt('dadsadsadsadfcs', NULL, 'dadsadsadsa'), aes_encrypt(NULL, NULL, NULL);

select aes_decrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1');
select aes_decrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', 'abcd');
create table test_encrypt(f1 text);
insert into test_encrypt select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef');
select aes_decrypt(f1, 'cdjsfj3713vdVHV1', '1234567890abcdef') from test_encrypt;
select aes_decrypt(f1, 'cdjsfj3713vdVHV1', '1234567890abcdef') = 'HuaweiGauss_234' from test_encrypt;
select aes_decrypt(aes_encrypt('~`!@#$%^&*()_+-=<>?,./;''“”""[]{}\|','cdjsfj3713vdVHV1', '1234567890abcdef'),'cdjsfj3713vdVHV1', '1234567890abcdef');
select aes_decrypt(NULL, NULL, 'dadsa'), aes_decrypt(NULL, 'dasdsa', 'dsadsa'), aes_decrypt('dadsad', NULL, 'dadsadsa');
select aes_decrypt(aes_encrypt('大家好','cdjsfj3713vdVHV1', '1234567890abcdef'),'cdjsfj3713vdVHV1', '1234567890abcdef');
select aes_decrypt(aes_encrypt(E'd\nsa\tdj\bsaf\Z\\dssa\ca\rs','dajd123FGBJG', '1234567890abcdef'),'dajd123FGBJG', '1234567890abcdef');
select aes_decrypt(aes_encrypt('~·！@#￥%……&*（）-=——+{}【】、|：‘’，。《》、？','12345dvghadCVBUJNF', '1234567890abcdef'),'12345dvghadCVBUJNF', '1234567890abcdef');
drop table test_encrypt;

set block_encryption_mode = 'aes-128-ofb';
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', 'abcd');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef123456');
select lengthb(aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef123456')), lengthb('HuaweiGauss_234');
select lengthb(aes_encrypt('1234567890', 'cdjsfj3713vdVHV1', '1234567890abcdef123456')), lengthb('1234567890');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef') = aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef123456');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890dgjahdjk');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef') = aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890dgjahdjk');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', NULL);
select aes_encrypt('HuaweiGauss_234', NULL, '1234567890dgjahdjk');
select aes_encrypt(NULL, 'cdjsfj3713vdVHV1', '1234567890dgjahdjk');
select aes_encrypt(NULL, NULL, '1234567890dgjahdjk');
select aes_encrypt(NULL, 'dadsads'), aes_encrypt('dadsadsadsadfcs', NULL), aes_encrypt(NULL, NULL);
select aes_encrypt(NULL, 'dadsads', 'dadsadsadsa'), aes_encrypt('dadsadsadsadfcs', NULL, 'dadsadsadsa'), aes_encrypt(NULL, NULL, NULL);

select aes_decrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1');
select aes_decrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', 'abcd');
create table test_encrypt(f1 text);
insert into test_encrypt select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef');
select aes_decrypt(f1, 'cdjsfj3713vdVHV1', '1234567890abcdef') from test_encrypt;
select aes_decrypt(f1, 'cdjsfj3713vdVHV1', '1234567890abcdef') = 'HuaweiGauss_234' from test_encrypt;
select aes_decrypt(aes_encrypt('~`!@#$%^&*()_+-=<>?,./;''“”""[]{}\|','cdjsfj3713vdVHV1', '1234567890abcdef'),'cdjsfj3713vdVHV1', '1234567890abcdef');
select aes_decrypt(NULL, NULL, 'dadsa'), aes_decrypt(NULL, 'dasdsa', 'dsadsa'), aes_decrypt('dadsad', NULL, 'dadsadsa');
select aes_decrypt(aes_encrypt('大家好','cdjsfj3713vdVHV1', '1234567890abcdef'),'cdjsfj3713vdVHV1', '1234567890abcdef');
select aes_decrypt(aes_encrypt(E'd\nsa\tdj\bsaf\Z\\dssa\ca\rs','dajd123FGBJG', '1234567890abcdef'),'dajd123FGBJG', '1234567890abcdef');
select aes_decrypt(aes_encrypt('~·！@#￥%……&*（）-=——+{}【】、|：‘’，。《》、？','12345dvghadCVBUJNF', '1234567890abcdef'),'12345dvghadCVBUJNF', '1234567890abcdef');
drop table test_encrypt;

set block_encryption_mode = 'aes-192-ofb';
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', 'abcd');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef123456');
select lengthb(aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef123456')), lengthb('HuaweiGauss_234');
select lengthb(aes_encrypt('1234567890', 'cdjsfj3713vdVHV1', '1234567890abcdef123456')), lengthb('1234567890');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef') = aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef123456');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890dgjahdjk');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef') = aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890dgjahdjk');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', NULL);
select aes_encrypt('HuaweiGauss_234', NULL, '1234567890dgjahdjk');
select aes_encrypt(NULL, 'cdjsfj3713vdVHV1', '1234567890dgjahdjk');
select aes_encrypt(NULL, NULL, '1234567890dgjahdjk');
select aes_encrypt(NULL, 'dadsads'), aes_encrypt('dadsadsadsadfcs', NULL), aes_encrypt(NULL, NULL);
select aes_encrypt(NULL, 'dadsads', 'dadsadsadsa'), aes_encrypt('dadsadsadsadfcs', NULL, 'dadsadsadsa'), aes_encrypt(NULL, NULL, NULL);

select aes_decrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1');
select aes_decrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', 'abcd');
create table test_encrypt(f1 text);
insert into test_encrypt select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef');
select aes_decrypt(f1, 'cdjsfj3713vdVHV1', '1234567890abcdef') from test_encrypt;
select aes_decrypt(f1, 'cdjsfj3713vdVHV1', '1234567890abcdef') = 'HuaweiGauss_234' from test_encrypt;
select aes_decrypt(aes_encrypt('~`!@#$%^&*()_+-=<>?,./;''“”""[]{}\|','cdjsfj3713vdVHV1', '1234567890abcdef'),'cdjsfj3713vdVHV1', '1234567890abcdef');
select aes_decrypt(NULL, NULL, 'dadsa'), aes_decrypt(NULL, 'dasdsa', 'dsadsa'), aes_decrypt('dadsad', NULL, 'dadsadsa');
select aes_decrypt(aes_encrypt('大家好','cdjsfj3713vdVHV1', '1234567890abcdef'),'cdjsfj3713vdVHV1', '1234567890abcdef');
select aes_decrypt(aes_encrypt(E'd\nsa\tdj\bsaf\Z\\dssa\ca\rs','dajd123FGBJG', '1234567890abcdef'),'dajd123FGBJG', '1234567890abcdef');
select aes_decrypt(aes_encrypt('~·！@#￥%……&*（）-=——+{}【】、|：‘’，。《》、？','12345dvghadCVBUJNF', '1234567890abcdef'),'12345dvghadCVBUJNF', '1234567890abcdef');
drop table test_encrypt;

set block_encryption_mode = 'aes-256-ofb';
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', 'abcd');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef123456');
select lengthb(aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef123456')), lengthb('HuaweiGauss_234');
select lengthb(aes_encrypt('1234567890', 'cdjsfj3713vdVHV1', '1234567890abcdef123456')), lengthb('1234567890');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef') = aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef123456');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890dgjahdjk');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef') = aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890dgjahdjk');
select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', NULL);
select aes_encrypt('HuaweiGauss_234', NULL, '1234567890dgjahdjk');
select aes_encrypt(NULL, 'cdjsfj3713vdVHV1', '1234567890dgjahdjk');
select aes_encrypt(NULL, NULL, '1234567890dgjahdjk');
select aes_encrypt(NULL, 'dadsads'), aes_encrypt('dadsadsadsadfcs', NULL), aes_encrypt(NULL, NULL);
select aes_encrypt(NULL, 'dadsads', 'dadsadsadsa'), aes_encrypt('dadsadsadsadfcs', NULL, 'dadsadsadsa'), aes_encrypt(NULL, NULL, NULL);

select aes_decrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1');
select aes_decrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', 'abcd');
create table test_encrypt(f1 text);
insert into test_encrypt select aes_encrypt('HuaweiGauss_234', 'cdjsfj3713vdVHV1', '1234567890abcdef');
select aes_decrypt(f1, 'cdjsfj3713vdVHV1', '1234567890abcdef') from test_encrypt;
select aes_decrypt(f1, 'cdjsfj3713vdVHV1', '1234567890abcdef') = 'HuaweiGauss_234' from test_encrypt;
select aes_decrypt(aes_encrypt('~`!@#$%^&*()_+-=<>?,./;''“”""[]{}\|','cdjsfj3713vdVHV1', '1234567890abcdef'),'cdjsfj3713vdVHV1', '1234567890abcdef');
select aes_decrypt(NULL, NULL, 'dadsa'), aes_decrypt(NULL, 'dasdsa', 'dsadsa'), aes_decrypt('dadsad', NULL, 'dadsadsa');
select aes_decrypt(aes_encrypt('大家好','cdjsfj3713vdVHV1', '1234567890abcdef'),'cdjsfj3713vdVHV1', '1234567890abcdef');
select aes_decrypt(aes_encrypt(E'd\nsa\tdj\bsaf\Z\\dssa\ca\rs','dajd123FGBJG', '1234567890abcdef'),'dajd123FGBJG', '1234567890abcdef');
select aes_decrypt(aes_encrypt('~·！@#￥%……&*（）-=——+{}【】、|：‘’，。《》、？','12345dvghadCVBUJNF', '1234567890abcdef'),'12345dvghadCVBUJNF', '1234567890abcdef');
drop table test_encrypt;

\c postgres
drop database b;