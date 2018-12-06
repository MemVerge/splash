# Plugin API

The plugin API of the shuffle manager is defined in Java.  User could implement
the API to support different storage for different kind of shuffle IO.
Here is a brief description of that each interface does.

For detail, please check the javadoc code.

## `StorageFactory` interface

This class defines the entry point for the storage/IO functions.  The methods of
this class could be divided into following categories.

* Metadata operations - returns the storage metadata like where to put the 
  shuffle files for each application.  And how many temp files or shuffle files
  do we have at certain time.
* Temp file generators - Create the temp files for IO.  Those temp files are
  divided into 3 kinds based on their usage:
  * Spill temp files
  * Data temp files
  * Index temp files
  Temp files will be committed to final shuffle output when the map task 
  completes.  Temp files should implement the `TmpShuffleFile` interface.
* Getting file reference - Retrieve the instance that represents the shuffle 
  file.  Those instances should implement `ShuffleFile` interface.
* Listeners - retrieve the collection of listeners of the Splash shuffle 
  manager.  Listeners could be defined to execute certain operations in certain
  shuffle stage.
* Cleanup - used to reset/clean up the shuffle output or temp files.

## `ShuffleFile` interface

The interface for all shuffle related files.  The interface looks like a normal
Java `File`.  And it contains generator functions for `InputStream`.

## `TmpShuffleFile` interface

This interface extends the `ShuffleFile` interface.  The main additional 
functions are `commit` and `recall`.  These methods are atomic methods that 
allow user to commit the shuffle output or recall it.  And it contains generator
functions for `OutputStream`.

## `ShuffleListener` interface

Define the listeners to invoke during the shuffle procedure.
