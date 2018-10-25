# Contributing

## How to contribute
Contributing is welcome in this project.  We are using a light-weight process
for all code changes.

Please file an issue describing the bug or feature you are trying to address 
before creating a PR(pull request).  It would help the maintainer to better 
understand the problem.

After a PR is created, online CI will be triggered automatically.

### Development model
Use [this branching model](https://nvie.com/posts/a-successful-git-branching-model/) 
as reference.
* Create an issue in github.
* Fork your own repo.
* Create a new branch from `development` for your development.
* Create a PR from your branch to the `development` branch
* Make sure your PR meets the PR criteria.
* Address the review comments.
* Squash and merge your PR in the rebase way.

### PR criteria
Please make sure your PR reaches following criteria:

* Include the issue number in your PR title like this:
  ```
  [SPLASH-05] Add HDFS implementation...
  ```  
  So that github will do the favor of creating a link from your PR to the issue.
* CI for the PR is passing.
* Make sure you have a test to cover the change.
* Test coverage of the overall project should not drop more than 0.5%.
* If you are a new contributor, make sure to add your name to the contributors'
  list at the bottom of this file.
  
### Release
Releases are published in github and central repository.
Each release should include a release note including:
* Major feature introduction
* Fixed issue list
* Known issue list
* Contributor list
* All release commits are available in `master` 
  
### Develop Storage Plugin
* Storage plugins should be developed in separate repositories for following 
  reasons.
  * De-couple release cycles.
  * Release the test burden for each plugin.
  * Do not force plugin to be open source.
  * Plugins may require different running privileges.
  
## Governance

We starts with BDFL.
* Existing maintainers work as gatekeepers for the PR.
* 2 approvals are required for each PR.
* Issues and proposals are discussed in Github issues.

## Contributors 

### MemVerge
* [Cedric Zhuang](https://github.com/jealous), cedric.zhuang@memverge.com
* [Sheperd Huang](https://github.com/sheperdh), sheperd.huang@memverge.com
* [Zeyi Qi](https://github.com/czoey), zeyi.qi@memverge.com
