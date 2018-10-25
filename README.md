# MV Shuffle Manager

The java native API for MVFS DMO operations.

## Build

* Use `mvn install` to build.  Use `-DskipTests=true` option to disable UT.
When the build process completes:
  * A fat jar will be generated at: `./target/jmvfs-<version>-shaded.jar`
  * You can find the unit test result in: `./target/surefire-reports`
  * You can find the coverage report in: `./target/site/jacoco` 

* Use `mvn clean` to clean the build output.

* Use `integration-test` or `mvn failsafe:integration-test -DskipIT=false`
  to run the integration tests.  Those tests should connect to the actual File 
  System.
  * IT result is available in: `./target/failsafe-reports`

* Use `mvn pmd:pmd` to run static code analysis.
  * Analysis report is available in: `./target/site/pmd.html`

