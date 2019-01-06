# Splash

A shuffle manager for Spark that supports different storage plugin.

## Vision
Supply a fast, flexible and reliable shuffle manager that allows user to plug in his/her favoriate backend storage and network frameworks for holding and exchanging shuffle data. 

## License
[Apache License Version 2.0](LICENSE)

## Project Motivation

*	Local shuffle has limitations on reliability and performance. Node or even just local storage failures may seriously impact the performance of the whole cluster.
*	The performance of local hard disk IO is affecting the overall performance when there is heavy shuffle.
*	There is no easy/general solution currently available to plug external storage into existing 
  shuffle service.

## Deployment
### Spark
* Include the jar file in your spark default configuration or task 
  configuration.
* Include the plugin jar in the same way.
* Switch to the Splash shuffle manager by adding the following option:
```
spark.shuffle.manager org.apache.spark.shuffle.sort.SplashShuffleManager
``` 
* The storage plugin could be switched at application level.
* Support both on-premise and cloud deployments.

## Release
* Release number follows [Semantic Versioning 2.0.0](https://semver.org/#semantic-versioning-200)
* Release is tracked in project's release page.

## Upgrade
* Public API is not fixed until we reach version 1.0.0.
* Do not promise backward compatibility if the first digit in version is changed.

## Service & Support
* Questions could be raised in github issue and tagged with `question`.
* Project documents are available in the

## Community
You could communicate with us in following ways:
* Start a new thread in [github issues](https://github.com/MemVerge/splash/issues)
* Check our slack group for [Splash](https://splash-headquarters.slack.com/)
* Request to join the WeChat group through [email](mailto://cedric.zhuang@memverge.com) 
  which includes your WeChat ID. 

## Build

* Use `mvn install` to build.  Use `-DskipTests=true` option to disable UT.
When the build process completes:
  * A fat jar will be generated at: `./target/splash-<version>-shaded.jar`
  * You can find the unit test result in: `./target/surefire-reports`
  * You can find the coverage report in: `./target/site/jacoco` 

* Use `mvn clean` to clean the build output.

* Use `integration-test` or `mvn failsafe:integration-test -DskipIT=false`
  to run the integration tests.  Those tests should connect to the actual File 
  System.
  * IT result is available in: `./target/failsafe-reports`

* Use `mvn pmd:pmd` to run static code analysis.
  * Analysis report is available in: `./target/site/pmd.html`

## Options
* `spark.shuffle.splash.storageFactory`:
  Specify the class name of your factory.  This class must implement 
  [`StorageFactory`](src/main/java/com/memverge/splash/StorageFactory.java)
* `spark.shuffle.splash.clearShuffleOutput`: Boolean value telling the shuffle
  manager whether to clear the shuffle output or not when the shuffle stage 
  completes.
   
## Plugin Development
Splash uses plugins to support different storage.  User could develop their own
storage plugins for the shuffle manager.  And different storage could be used
based on the usage of the file.  For detail, please check the 
[Plugin API](doc/Plugin_API.md) document.

The Splash project is currently released with two default plugins:
* the plugin for shared file system such as NFS:
  `com.memverge.splash.shared.SharedFSFactory`
* plugin for Spark block manager with local disk storage:
  `org.apache.spark.shuffle.local.LocalStorageFactory`

These plugins also serve as examples for developers to develop his/her own storage plugins.
