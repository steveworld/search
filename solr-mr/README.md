# Introduction

Scalable, fault tolerant, batch oriented system for processing large numbers of records contained in files 
that are stored on HDFS into free text search indexes stored on HDFS.

For documentation see the [Cloudera Wiki](https://wiki.cloudera.com/display/engineering/Solr+MR).

The main public tools are TikaIndexerTool and HdfsFindShell, as shown below.

# TikaIndexerTool

```
usage: hadoop [GenericOptions]... jar solr-mr-*-job.jar 
       [--help] [--inputlist URI] --outputdir HDFS_URI
       --solrhomedir DIR [--mappers INTEGER] [--shards INTEGER]
       [--fairschedulerpool STRING] [--verbose] [--norandomize]
       [--identitytest] [HDFS_URI [HDFS_URI ...]]

Map Reduce job that creates a set of Solr index shards from a list of 
input files and writes the indexes into HDFS, in a scalable and fault-
tolerant manner.

positional arguments:
  HDFS_URI               HDFS URI of file or dir to index (default: [])

optional arguments:
  --help, -h             show this help message and exit
  --inputlist URI        Local URI or HDFS URI of a file containing a list 
                         of HDFS URIs to index, one URI per line. If '-' 
                         is specified, URIs are read from the standard 
                         input. Multiple --inputlist arguments can be 
                         specified
  --outputdir HDFS_URI   HDFS directory to write Solr indexes to
  --solrhomedir DIR      Local dir containing Solr conf/ and lib/
  --mappers INTEGER      Maximum number of MR mapper tasks to use 
                         (default: 1)
  --shards INTEGER       Number of output shards to use (default: 1)
  --fairschedulerpool STRING
                         Name of MR fair scheduler pool to submit job to
  --verbose, -v          Turn on verbose output (default: false)
  --norandomize          undocumented and subject to removal without 
                         notice (default: false)
  --identitytest         undocumented and subject to removal without 
                         notice (default: false)

Generic options supported are
-conf <configuration file>     specify an application configuration file
-D <property=value>            use value for given property
-fs <local|namenode:port>      specify a namenode
-jt <local|jobtracker:port>    specify a job tracker
-files <comma separated list of files>    specify comma separated files to be copied to the map reduce cluster
-libjars <comma separated list of jars>    specify comma separated jar files to include in the classpath.
-archives <comma separated list of archives>    specify comma separated archives to be unarchived on the compute machines.

The general command line syntax is
bin/hadoop command [genericOptions] [commandOptions]

Examples: 

sudo -u hdfs hadoop --config /etc/hadoop/conf.cloudera.mapreduce1 jar solr-mr-*-job.jar  --solrhomedir /home/foo/solr --outputdir hdfs:///user/foo/tikaindexer-output hdfs:///user/foo/tikaindexer-input
```

# HdfsFindShell

This is essentially the HDFS version of the Linux file system 'find' command. 
For example, it can be used to find all .pdf files matching a given glob pattern that have been created/modified since midnight.
The command generates a list of output file names that can be piped into the TikaIndexerTool described above, using the --inputlist option.

```
# hadoop jar solr-mr-1.0-SNAPSHOT.jar org.apache.solr.hadoop.tika.HdfsFindShell -help
Usage: hadoop fs [generic options]
  [-find <path> ... <expression> ...]
  [-help [cmd ...]]
  [-usage [cmd ...]]

-find <path> ... <expression> ...:  Finds all files that match the specified expression and applies selected actions to them.
    
    The following primary expressions are recognised:
      -atime n
      -amin n
        Evaluates as true if the file access time subtracted from
        the start time is n days (or minutes if -amin is used).
    
      -blocks n
        Evaluates to true if the number of file blocks is n.
    
      -class classname [args ...]
        Executes the named expression class.
    
      -depth
        Always evaluates to true. Causes directory contents to be
        evaluated before the directory itself.
    
      -empty
        Evaluates as true if the file is empty or directory has no
        contents.
    
      -exec command [argument ...]
      -ok command [argument ...]
        Executes the specified Hadoop shell command with the given
        arguments. If the string {} is given as an argument then
        is replaced by the current path name.  If a {} argument is
        followed by a + character then multiple paths will be
        batched up and passed to a single execution of the command.
        A maximum of 500 paths will be passed to a single
        command. The expression evaluates to true if the command
        returns success and false if it fails.
        If -ok is specified then confirmation of each command shall be
        prompted for on STDERR prior to execution.  If the response is
        'y' or 'yes' then the command shall be executed else the command
        shall not be invoked and the expression shall return false.
    
      -group groupname
        Evaluates as true if the file belongs to the specified
        group.
    
      -mtime n
      -mmin n
        Evaluates as true if the file modification time subtracted
        from the start time is n days (or minutes if -mmin is used)
    
      -name pattern
      -iname pattern
        Evaluates as true if the basename of the file matches the
        pattern using standard file system globbing.
        If -iname is used then the match is case insensitive.
    
      -newer file
        Evaluates as true if the modification time of the current
        file is more recent than the modification time of the
        specified file.
    
      -nogroup
        Evaluates as true if the file does not have a valid group.
    
      -nouser
        Evaluates as true if the file does not have a valid owner.
    
      -perm [-]mode
      -perm [-]onum
        Evaluates as true if the file permissions match that
        specified. If the hyphen is specified then the expression
        shall evaluate as true if at least the bits specified
        match, otherwise an exact match is required.
        The mode may be specified using either symbolic notation,
        eg 'u=rwx,g+x+w' or as an octal number.
    
      -print
      -print0
        Always evaluates to true. Causes the current pathname to be
        written to standard output. If the -print0 expression is
        used then an ASCII NULL character is appended.
    
      -prune
        Always evaluates to true. Causes the find command to not
        descend any further down this directory tree. Does not
        have any affect if the -depth expression is specified.
    
      -replicas n
        Evaluates to true if the number of file replicas is n.
    
      -size n[c]
        Evaluates to true if the file size in 512 byte blocks is n.
        If n is followed by the character 'c' then the size is in bytes.
    
      -type filetype
        Evaluates to true if the file type matches that specified.
        The following file type values are supported:
        'd' (directory), 'l' (symbolic link), 'f' (regular file).
    
      -user username
        Evaluates as true if the owner of the file matches the
        specified user.
    
    The following operators are recognised:
      expression -a expression
      expression -and expression
      expression expression
        Logical AND operator for joining two expressions. Returns
        true if both child expressions return true. Implied by the
        juxtaposition of two expressions and so does not need to be
        explicitly specified. The second expression will not be
        applied if the first fails.
    
      ! expression
      -not expression
        Evaluates as true if the expression evaluates as false and
        vice-versa.
    
      expression -o expression
      expression -or expression
        Logical OR operator for joining two expressions. Returns
        true if one of the child expressions returns true. The
        second expression will not be applied if the first returns
        true.

-help [cmd ...]:  Displays help for given command or all commands if none
    is specified.

-usage [cmd ...]: Displays the usage for given command or all commands if none
    is specified.

Generic options supported are
-conf <configuration file>     specify an application configuration file
-D <property=value>            use value for given property
-fs <local|namenode:port>      specify a namenode
-jt <local|jobtracker:port>    specify a job tracker
-files <comma separated list of files>    specify comma separated files to be copied to the map reduce cluster
-libjars <comma separated list of jars>    specify comma separated jar files to include in the classpath.
-archives <comma separated list of archives>    specify comma separated archives to be unarchived on the compute machines.

The general command line syntax is
bin/hadoop command [genericOptions] [commandOptions]
```

