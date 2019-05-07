# Splash

[![travis-ci](https://img.shields.io/travis/MemVerge/splash/master.svg)](https://travis-ci.org/MemVerge/splash)
[![codecov](https://img.shields.io/codecov/c/gh/MemVerge/splash/master.svg)](https://codecov.io/gh/MemVerge/splash)
[![license](https://img.shields.io/github/license/MemVerge/splash.svg)](LICENSE)

A shuffle manager for Spark that supports different storage plugins.

The motivation of this project is to supply a fast, flexible and reliable 
shuffle manager that allows the user to plug in his/her favorite backend storage 
and network frameworks for holding and exchanging shuffle data. 

In general, the current shuffle manager in Spark has some shortcomings.

* The local shuffle data have limitations on reliability and performance. 
  * Losing a single node can break the data integrity of the entire cluster.
  * It is difficult to containerize the application.
  * In order to improve the shuffle read/write performance, you must upgrade 
    each server in the cluster.
  * the overall performance of the shuffle stage is affected by the performance 
    of local disk IO when there is heavy shuffling.
* There is no easy/general solution to plugin external storage to the shuffle 
  service.
  

We want to address these issues in this shuffle manager.

---

* [License](#license)
* [Deployment](#deployment)
* [Release](#release)
* [Upgrade](#upgrade)
* [Service & Support](#service--support)
* [Community](#community)
* [Contributing](#contributing)
* [Build](#build)
* [Options](#options)
* [Plugin Development](#plugin-development)

---

## License
[Apache License Version 2.0](LICENSE)

## Deployment
By default, we support Spark 2.3.2_2.11 with Hadoop 2.7.  
If you want to generate a build with a different Spark version, you need to modify 
these version parameters in `pom.xml` 
* `spark.version`
* `hadoop.version`
* `scala.version`

Check the [Build](#build) section for how to generate your customized jar.

### Spark
* You need to include the Splash jar file in your spark default configuration 
  or task configuration.  Make sure you choose the one that is aligned with your 
  Spark and Scala version.  Typically, you only need to add two configurations 
  in your `spark-defaults.conf`
  
```
spark.driver.extraClassPath /path/to/splash.jar
spark.executor.extraClassPath /path/to/splash.jar
```

* You can include the plugin jar in the same way.
* You can configure your Spark application to use the Splash shuffle manager 
  by adding the following option:

```
spark.shuffle.manager org.apache.spark.shuffle.SplashShuffleManager
```

* The storage plugin is tunable at the application level.  The user can specify 
  different storage implementations for different applications.
* Support both on-premise and cloud deployments.

## Release
* Release numbering follows [Semantic Versioning 2.0.0](https://semver.org/#semantic-versioning-200)
* Releases are available in project's release page.

## Upgrade
Although the basic functionality of the project has been verified, we still feel 
that the public API might be modified when more storage plugins are developed. 
Therefore:
* The public API may change until we reach version 1.0.0.

According to the definition of semantic versioning 2.0.0, we do not promise 
backward compatibility if the first digit in the version is changed.

## Service & Support
* Please raise your question in the project's issue page and tag it with 
  `question`.
* Project documents are available in the `doc` folder.

## Community
You can communicate with us in following ways:
* Start a new thread in [Github issues](https://github.com/MemVerge/splash/issues), 
  recommended.
* Request to join the WeChat group through [email](mailto://cedric.zhuang@memverge.com) 
  and make sure you include your WeChat ID in the mail.

## Contributing
Please check the [Contributing](CONTRIBUTING.md) document for details.

## Build

* Use `mvn install` to build the project.  Optionally, you could use 
  `-DskipTests=true` to disable the unit tests.

  When the build process completes:
  * A standard jar will be generated at: `./target/splash-<version>.jar`.  This
    jar is what you need to deploy to your Spark environment.
  * A fat jar will be generated at: `./target/splash-<version>-shaded.jar`
  * You can find the unit test result in: `./target/surefire-reports`
  * You can find the coverage report in: `./target/site/jacoco` 

* Use `mvn clean` to clean the build output.

* Use `integration-test` or `mvn failsafe:integration-test -DskipIT=false`
  to run the integration tests.  Those tests should connect to the actual File 
  System.  You could also modify the test source code to test your own storage 
  plugin.
  * Once the tests complete, the results are available in: 
    `./target/failsafe-reports`

* Use `mvn pmd:pmd` to run static code analysis.

  * Analysis report is available in: `./target/site/pmd.html`

## Options
* `spark.shuffle.splash.storageFactory` specifies the class name of your 
  factory.  This class must implement 
  [`StorageFactory`](src/main/java/com/memverge/splash/StorageFactory.java)
* `spark.shuffle.splash.clearShuffleOutput` is a boolean value telling the 
  shuffle manager whether to clear the shuffle output when the shuffle stage 
  completes.
  
## Plugin Development
Splash uses plugins to support different types of storage systems.  The user can 
develop their own storage plugins for the shuffle manager.  The user can use 
different types of storage system based on the usage of the file.  For details, 
please check our [design document](doc/Design.md).

The Splash project is currently released with a default plugin:
* the plugin for shared file systems like NFS is implemented by:
  `com.memverge.splash.shared.SharedFSFactory`

This plugin serves as an example for developers to develop their own 
storage plugins.

### Deploy Shared Folder Storage Plugin
Take NFS as an example, here are the steps to configure Splash with the shared folder plugin.
* Update the configurations in `spark-defaults.conf`:

```
# add the Splash jar to the classpath
spark.driver.extraClassPath /path/to/splash.jar
spark.executor.extraClassPath /path/to/splash.jar

# set shuffle manager and storage plugin
spark.shuffle.manager org.apache.spark.shuffle.SplashShuffleManager
spark.shuffle.splash.storageFactory com.memverge.splash.shared.SharedFSFactory

# set the location of your shared folder
spark.shuffle.splash.folder /your/share/folder
```
* Make sure that all your Spark nodes can access the shared folder you specified in the configuration file.
* Run some sample Spark applications and you should be able to observe that the application folder is created in a shared folder.
