# Contributing

## How to contribute

We greatly appreciate and welcome your contributions to Splash!  We are using a 
light-weight process for all code changes.

Before making any actual code change, please file an issue describing the 
feature or bug you are trying to address.  It would help the maintainer to 
better understand your efforts and avoid any duplicated work.

You could start working on the issue when the issue is confirmed, and the issue 
is assigned to you.

Here is the development model we recommend.

### Development model

We use [this branching model](https://nvie.com/posts/a-successful-git-branching-model/) 
as a reference.  Each step is described below:
* Create an issue on Github.
* Wait for the issue is confirmed and assigned to you.
* Fork your own repo if you haven't done so.
* Create a new branch from the latest `development` branch in the main repo.
* Make your changes in your own feature/bugfix branch.
* To avoid conflicts, please try to rebase your feature/bugfix branch to the 
  latest `development` branch from time to time.
* When you are done, create a pull request (PR) from your branch to the 
  `development` branch in the main repo.
* Address the review comments and make sure your PR meets the PR criteria 
  listed below.
* The project maintainer will squash and merge your PR via rebasing.

### PR criteria
Please make sure your PR satisfies the following criteria:

* Include the issue number in your PR title using the following format:
  ```
  [SPLASH-05] Add HDFS implementation...
  ```
  So that Github will do the favor of creating a link from your PR to the issue.
* Make sure that all the automatic verification for the PR is passing.
* Make sure you have a test to cover the change.
* Test coverage of the overall project should not drop more than 0.5%.
* If you are a new contributor, make sure to add your name to the contributors'
  list at the bottom of this file.
### Release
Releases are published in Github and central repository.
Each release includes a release note including:

* Major feature introduction
* Fixed issue list
* Known issue list
* Contributor list

All releases are available in the `master` branch.

### Develop Storage Plugin

Storage plugins should be developed in separate repositories for following reasons.
  * De-couple release cycles.
  * Release the test burden for each plugin.
  * Do not force plugin to be open source.
  * Plugins may require different running privileges.

## Governance

We have a simple governance model.  There are two roles:

* Maintainer
* Contributor

Maintainers work as gate keepers.  Each PR requires at least the approval of 
two maintainers to get into the main repo.

Maintainers will be responsible for nominate the new maintainer from the 
contributors.  When the nomination is approved by most of the existing 
maintainers, the contributor becomes a new maintainer.

Maintainers could voluntarily give up their maintainer role when they want to 
step away from the project.

### Maintainers
* [Yue Li](https://github.com/yuelimv), yue.li@memverge.com
* [Jie Yu](https://github.com/jieyumv), jie.yu@memverge.com
* [Cedric Zhuang](https://github.com/jealous), cedric.zhuang@memverge.com
* [Sheperd Huang](https://github.com/sheperdh), sheperd.huang@memverge.com
* [Haiyan Wang](https://github.com/wanghy73), haiyan.wang@memverge.com

### Contributors
* [Zeyi Qi](https://github.com/qzyse2017), zyck2018@gmail.com
