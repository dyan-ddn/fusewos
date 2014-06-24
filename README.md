fusewos
=======

A Linux FUSE based file system layer for DDN's WOS object storage (ddn.com).  

fusewos stores a file's data body in WOS storage cluster, but leaves a stub file with meta data in the local directory tree.  WOS storage cluster is transparent to file system applications.

fusewos seperates file system meta data from file body.  It is especially good for big data use cases.  This opens the door to optimize file IO by taking advantage of storage methods that are tuned for small data IO and big data IO.

fusewos can work on top of local, clustered or network file systems, thanks to FUSE.

fusewos supports file versioning.

fusewos supports trash can per mount point.

fusewos supports file system snapshots.

fusewos supports an option for directory tree backup.

fusewos uses one inode for each file in most cases, which means for ext4 file system, that's 4KB disk usage per file by default.  One 4TB disk can store 1 Billion files.  1KB is most likely enough for fusewos though.  Easy disk usage calculation, no mystery here in sizing for storage needs.

All conventional tools can be used with the meta data directory for searching, examination, backup, sync, and etc.


Runtime Dependencies
--------------------

**1. Operating System**

fusewos is initially developed and tested on CentOS 6.4 64-bit platform.  It should be quite trivial to make it work in other distributions as well.  

The following contents in this readme assume CentOS 6.4 64-bit is the host operating system.  You can download the mini install ISO from here: http://mirror.symnds.com/distributions/CentOS-vault/6.4/isos/x86_64/CentOS-6.4-x86_64-minimal.iso


**2. C++ WOSLib 2.x**

C++ WOSLib shared library can be found in the WOS C++ Dev Kit which can be downloaded from WOS cluster admin WebGUI.  fusewos was tested with WOSLib version 2.2.2, but other versions shall work too.

Download the C++ Dev Kit, find and copy the C++ WOSLib shared library to /usr/lib64 directory.  Here is an example to do so:

    cp -a /root/downloads/dist/lib64/libwos_cpp.so* /usr/lib64


**3. FUSE Library**

Fuse library is needed for fusewos to run.  It can be installed in CentOS 6.4 with:

    yum install -y fuse-libs
    
The FUSE library will be installed in directory /lib64 by yum.


**4. Test if Needed Libraries are Installed**

Run the following command to see if the needed shared libraries are installed in the OS:

    ldd /path/to/the/fusewos

Here is an example that shows both woslib and fuse libraries are missing in the shared library searching paths:

    [root@localhost cpp]# ldd /usr/local/bin/fusewos
        linux-vdso.so.1 =>  (0x00007fffda5ff000)
        libwos_cpp.so => not found
        libfuse.so.2 => not found
        libstdc++.so.6 => /usr/lib64/libstdc++.so.6 (0x00007ffdea545000)
        libm.so.6 => /lib64/libm.so.6 (0x00007ffdea2c1000)
        libgcc_s.so.1 => /lib64/libgcc_s.so.1 (0x00007ffdea0aa000)
        libc.so.6 => /lib64/libc.so.6 (0x00007ffde9d16000)
        libpthread.so.0 => /lib64/libpthread.so.0 (0x00007ffde9af9000)
        /lib64/ld-linux-x86-64.so.2 (0x00007ffdea856000)

Here is an example if all installed properly:

    [root@localhost wosfs]# ldd /usr/local/bin/fusewos
        linux-vdso.so.1 =>  (0x00007fffe8f3e000)
        libwos_cpp.so => /usr/lib64/libwos_cpp.so (0x00007fddf5989000)
        libfuse.so.2 => /lib64/libfuse.so.2 (0x00007fddf5753000)
        libstdc++.so.6 => /usr/lib64/libstdc++.so.6 (0x00007fddf544c000)
        libm.so.6 => /lib64/libm.so.6 (0x00007fddf51c8000)
        libgcc_s.so.1 => /lib64/libgcc_s.so.1 (0x00007fddf4fb2000)
        libc.so.6 => /lib64/libc.so.6 (0x00007fddf4c1d000)
        libpthread.so.0 => /lib64/libpthread.so.0 (0x00007fddf4a00000)
        /lib64/ld-linux-x86-64.so.2 (0x00007fddf5dfc000)
        librt.so.1 => /lib64/librt.so.1 (0x00007fddf47f8000)
        libdl.so.2 => /lib64/libdl.so.2 (0x00007fddf45f3000)


Installation
------------
There is only one binary executable to install, fusewos.  You can pull it from the latest sub directory in releases directory.  

Recommend to copy it under directory /usr/local/bin.  Here is an example to do so:

    cp /root/downloads/fusewos /usr/local/bin


Test Run
--------
**1. Create a mount point for fusewos to mount the name space under it**

Here is an example to do so:

    mkdir /mnt/fusewos


**2. Create a directory in host file system tree to map it with the mount point**

This directory will hold all the stub file file system structure.  Here is an exmaple to do so:

    mkdir /gpfs0/fusewos


**3. Mount the file system**

Run the following command to get the fusewos file system mounted:

    fusewos mountpoint -l <local fs stub file directory> -w <WOS Cluster IP address> -p <WOS Policy> \
            -s -f -o big_writes &

Note:

   - FUSE option "-s -f -o big_writes" are mandatory
   - "&" at the end of line is recommended to put it in back ground
   - The other options are self explained
   - may see message "fuse: warning: library too old, some operations may not not work" pops up.  It's due to the FUSE library is older than the one with which the fusewos binary was linked in build time, which is version 2.9.3, the newest as of June, 2014.  It's likely going to be running fine as the FUSE API used by fusewos is backward compatible at least with version 2.8.3.

Here is an example to run the command:

    fusewos /mnt/fusewos -l /gpfs0/fusewos/ -w 10.44.34.73 -p default -s -f -o big_writes &

**4. Copy a file to the mount point**

Here is an example:

    cp /etc/services /mnt/fusewos

Once the copy is done, file "services" will show up in both directory /mnt/fusewos and /gpfs0/fusewos.  The difference is:

- when you read back the file from directory /mnt/fusewos, you will get the file content, which is the same as original file at /etc/services.  This indicates the file body of "services" is read back intact from WOS cluster.

- when you read back the file from directory /gpfs0/fusewos, you will get the stub file content, similar to the following:

Colume 1: magic word "WOSWOS" specified in the command line

Column 2: WOS OID

Column 3: WOS Object/file length

Column 4: timestamp, seconds since 1970-01-01 00:00:00 UTC

Column 5: WOS Cluster IP address

Column 6: WOS policy used to store this file/object

    [root@localhost wosfs]# cat /gpfs0/fusewos/services
    WOSWOS lBkHWZxhDPjKseBkA3lr93Sy-GFuGL5RZBXlE_TL 641020 1402858866 10.44.34.73 default

File Versioning
---------------
When a file is re-written in fusewos file system, a new line of meta data is appended at the end of the stub file.

Here is an example:

    [root@linux-client-01 tmp]# cp /etc/services /mnt/fuse/
    [root@linux-client-01 tmp]# cp /etc/services /mnt/fuse/
    cp: overwrite `/mnt/fuse/services'? y
    [root@linux-client-01 tmp]# cat /wosfs/
    services         .WOSFS_TrashCan/ .WOS_snapshots/  
    [root@linux-client-01 tmp]# cat /wosfs/services 
    WOSWOS iDjAIhQmBk3PI0CyYKzab2NQcOfYb8HyAFhoJh-B 641020 1403646913 10.44.34.73 a_test01
    WOSWOS kDlGQBRgBivvJyCXywVG90BtgOYAY2gAr0T2AFFB 641020 1403646917 10.44.34.73 a_test01
                                                           ----------
                                                               ^
                                                               |_______ time stamp: date -d @1403646917 to get
                                                                                    the human readable format

When reading back the file within fusewos file system, the last line of the stub file is queried to get the file content from WOS core storage cluster.

If need to revert to an older version, just move the line correspondent to the version needed to the last line in the stub file.

Trash Can
---------
When a file or directory is deleted in fusewos file system, a line of original stub file path is appended at the end of the stub file, and the stub file(s) are moved into a folder at /<fusewos mount point>/.WOSFS_TrashCan/.

Here is an example of deleting a file in fusewos file system

    [root@linux-client-01 tmp]# rm /mnt/fuse/services
    rm: remove regular file `/mnt/fuse/services'? y
    [root@linux-client-01 tmp]# ls -l /mnt/fuse/.WOSFS_TrashCan/services.1403647595.551628711
    -rw-r--r-- 1 root root 641020 Jun 24 15:06 /mnt/fuse/.WOSFS_TrashCan/services.1403647595.551628711
    [root@linux-client-01 tmp]# cat /wosfs/.WOSFS_TrashCan/services.1403647595.551628711 
    WOSWOS iDjAIhQmBk3PI0CyYKzab2NQcOfYb8HyAFhoJh-B 641020 1403646913 10.44.34.73 a_test01
    WOSWOS kDlGQBRgBivvJyCXywVG90BtgOYAY2gAr0T2AFFB 641020 1403646917 10.44.34.73 a_test01
    WOSFS original path: /wosfs/services

If need to recover the file to old path or a different path, just move the stub file out of the trash can folder.

If a file in trash can folder is deleted via fusewos file system mount point, all versions of the file as listed in the stub file are deleted from WOS core cluster.



