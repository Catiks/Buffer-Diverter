## Buffer-Diverter

This is the source code of buffer-diverter

### OS and Software Dependency Requirements

The following OSs are supported:

- CentOS 7.6, openEuler-22.03-LTS (x86 architecture)

- openEuler-20.03-LTS (aarch64 architecture)


The following table lists the software requirements for compiling the openGauss.

You are advised to use the default installation packages of the following dependent software in the listed OS installation CD-ROMs or sources. If the following software does not exist, refer to the recommended versions of the software.

Software dependency requirements are as follows:

| Software      | Recommended Version |
| ------------- | ------------------- |
| libaio-devel  | 0.3.109-13          |
| flex          | 2.5.31 or later     |
| bison         | 2.7-4               |
| ncurses-devel | 5.9-13.20130511     |
| glibc-devel   | 2.17-111            |
| patch         | 2.7.1-10            |
| lsb_release   | 4.1                 |

From the following website, you can obtain the binarylibs we have compiled. Please unzip it and rename to **binarylibs** after you download.

<table>
    <tr>
        <td>gcc version</td>
        <td>download url</td>
    </tr>
    <tr>
        <td>gcc7.3</td>
        <td rowspan=1>
           <strong>openEuler_arm:</strong> <a href="https://opengauss.obs.cn-south-1.myhuaweicloud.com/latest/binarylibs/gcc7.3/openGauss-third_party_binarylibs_openEuler_arm.tar.gz">https://opengauss.obs.cn-south-1.myhuaweicloud.com/latest/binarylibs/gcc7.3/openGauss-third_party_binarylibs_openEuler_arm.tar.gz</a><br/>
            <strong>openEuler_x86:</strong> <a href="https://opengauss.obs.cn-south-1.myhuaweicloud.com/latest/binarylibs/gcc7.3/openGauss-third_party_binarylibs_openEuler_x86_64.tar.gz">https://opengauss.obs.cn-south-1.myhuaweicloud.com/latest/binarylibs/gcc7.3/openGauss-third_party_binarylibs_openEuler_x86_64.tar.gz</a><br/>
            <strong>Centos_x86:</strong> <a href="https://opengauss.obs.cn-south-1.myhuaweicloud.com/latest/binarylibs/gcc7.3/openGauss-third_party_binarylibs_Centos7.6_x86_64.tar.gz">https://opengauss.obs.cn-south-1.myhuaweicloud.com/latest/binarylibs/gcc7.3/openGauss-third_party_binarylibs_Centos7.6_x86_64.tar.gz</a><br/>
            <strong>openEuler 22.03 arm:</strong> <a href="https://opengauss.obs.cn-south-1.myhuaweicloud.com/latest/binarylibs/gcc7.3/openGauss-third_party_binarylibs_openEuler_arm.tar.gz">https://opengauss.obs.cn-south-1.myhuaweicloud.com/latest/binarylibs/gcc7.3/openGauss-third_party_binarylibs_openEuler_2203_arm.tar.gz</a><br/>
            <strong>openEuler 22.03 x86:</strong> <a href="https://opengauss.obs.cn-south-1.myhuaweicloud.com/latest/binarylibs/gcc7.3/openGauss-third_party_binarylibs_openEuler_2203_x86_64.tar.gz">https://opengauss.obs.cn-south-1.myhuaweicloud.com/latest/binarylibs/gcc7.3/openGauss-third_party_binarylibs_openEuler_2203_x86_64.tar.gz</a></td>
        </tr>
        <td>gcc10.3</td>
        <td rowspan=1>
           <strong>openEuler_arm:</strong> <a href="https://opengauss.obs.cn-south-1.myhuaweicloud.com/latest/binarylibs/gcc10.3/openGauss-third_party_binarylibs_openEuler_arm.tar.gz">https://opengauss.obs.cn-south-1.myhuaweicloud.com/latest/binarylibs/gcc10.3/openGauss-third_party_binarylibs_openEuler_arm.tar.gz</a><br/>
            <strong>openEuler_x86:</strong> <a href="https://opengauss.obs.cn-south-1.myhuaweicloud.com/latest/binarylibs/gcc10.3/openGauss-third_party_binarylibs_openEuler_x86_64.tar.gz">https://opengauss.obs.cn-south-1.myhuaweicloud.com/latest/binarylibs/gcc10.3/openGauss-third_party_binarylibs_openEuler_x86_64.tar.gz</a><br/>
            <strong>Centos_x86:</strong> <a href="https://opengauss.obs.cn-south-1.myhuaweicloud.com/latest/binarylibs/gcc10.3/openGauss-third_party_binarylibs_Centos7.6_x86_64.tar.gz">https://opengauss.obs.cn-south-1.myhuaweicloud.com/latest/binarylibs/gcc10.3/openGauss-third_party_binarylibs_Centos7.6_x86_64.tar.gz</a><br/>
            <strong>openEuler 22.03 arm:</strong> <a href="https://opengauss.obs.cn-south-1.myhuaweicloud.com/latest/binarylibs/gcc10.3/openGauss-third_party_binarylibs_openEuler_2203_arm.tar.gz">https://opengauss.obs.cn-south-1.myhuaweicloud.com/latest/binarylibs/gcc10.3/openGauss-third_party_binarylibs_openEuler_2203_arm.tar.gz</a><br/>
            <strong>openEuler 22.03 x86:</strong> <a href="https://opengauss.obs.cn-south-1.myhuaweicloud.com/latest/binarylibs/gcc10.3/openGauss-third_party_binarylibs_openEuler_2203_x86_64.tar.gz">https://opengauss.obs.cn-south-1.myhuaweicloud.com/latest/binarylibs/gcc10.3/openGauss-third_party_binarylibs_openEuler_2203_x86_64.tar.gz</a></td>
        </tr>
    </tr>
</table>

### Compiling by Command

1. Run the following script to obtain the system version:

   ```
   [user@linux openGauss-server]$ sh src/get_PlatForm_str.sh
   ```

   > **NOTICE:** 
   >
   > - The command output indicates the OSs supported by the openGauss. The OSs supported by the openGauss are centos7.6_x86_64 and openeuler_aarch64.
   > - If **Failed** or another version is displayed, the openGauss does not support the current operating system.

2. Configure environment variables, add **____** based on the code download location, and replace *** with the result obtained in the previous step.

   ```
   export CODE_BASE=________     # Path of the openGauss-server file
   export BINARYLIBS=________    # Path of the binarylibs file
   export GAUSSHOME=$CODE_BASE/dest/
   export GCC_PATH=$BINARYLIBS/buildtools/***/gcc7.3/
   export CC=$GCC_PATH/gcc/bin/gccexport CXX=$GCC_PATH/gcc/bin/g++
   export LD_LIBRARY_PATH=$GAUSSHOME/lib:$GCC_PATH/gcc/lib64:$GCC_PATH/isl/lib:$GCC_PATH/mpc/lib/:$GCC_PATH/mpfr/lib/:$GCC_PATH/gmp/lib/:$LD_LIBRARY_PATH
   export PATH=$GAUSSHOME/bin:$GCC_PATH/gcc/bin:$PATH

   ```

3. Select a version and configure it.

   **debug** version:

   ```
   ./configure --gcc-version=7.3.0 CC=g++ CFLAGS='-O0' --prefix=$GAUSSHOME --3rd=$BINARYLIBS --enable-debug --enable-cassert --enable-thread-safety --without-readline --without-zlib
   ```

   **release** version:

   ```
   ./configure --gcc-version=7.3.0 CC=g++ CFLAGS="-O2 -g3" --prefix=$GAUSSHOME --3rd=$BINARYLIBS --enable-thread-safety --without-readline --without-zlib
   ```

   **memcheck** version:

   ```
   ./configure --gcc-version=7.3.0 CC=g++ CFLAGS='-O0' --prefix=$GAUSSHOME --3rd=$BINARYLIBS --enable-debug --enable-cassert --enable-thread-safety --without-readline --without-zlib --enable-memory-check
   ```

   > **NOTICE:** 
   >
   > 1. *[debug | release | memcheck]* indicates that three target versions are available. 
   > 2. On the ARM-based platform, **-D__USE_NUMA** needs to be added to **CFLAGS**.
   > 3. On the **ARMv8.1** platform or a later version (for example, Kunpeng 920), **-D__ARM_LSE** needs to be added to **CFLAGS**.
   > 4. If **binarylibs** is moved to **openGauss-server** or a soft link to **binarylibs** is created in **openGauss-server**, you do not need to specify the **--3rd** parameter. However, if you do so, please note that the file is easy to be deleted by the `git clean` command.
   > 5. To build with mysql_fdw, add **--enable-mysql-fdw** when configure. Note that before build mysql_fdw, MariaDB's C client library is needed.
   > 6. To build with oracle_fdw, add **--enable-oracle-fdw** when configure. Note that before build oracle_fdw, Oracle's C client library is needed.

4. Run the following commands to compile openGauss:

   ```
   [user@linux openGauss-server]$ make -sj
   [user@linux openGauss-server]$ make install -sj
   ```

5. If the following information is displayed, the compilation and installation are successful:

   ```
   openGauss installation complete.
   ```

   The software installation path after compilation is **$GAUSSHOME**.

   The compiled binary files are stored in **$GAUSSHOME/bin**.

### System and Benchmark

1. Benchmark
    The benchmark in folder **benchmarksql-5.0**, first, run **gsql -d postgres -r -f make_user.sql** to create the user and database, then ./runDatabaseBuild.sh props.opengauss.80w.xxx to create the dataset, then run ./runBenchmark.sh props.opengauss.80w.xxx to run the benchmark, where xxx is the user name.
2. System
    The system in folder **monitor**, first build the server **go build**, then run **./ODBCtest**, then open the web monitor in brower in port 8877.