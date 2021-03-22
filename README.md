mongo-tools-common
===================================

A collection of packages shared by the tools and mongomirror.

**Note**: This project has been deprecated as of [TOOLS-2802](https://jira.mongodb.org/browse/TOOLS-2802) and currently supports only the [mongo-tools/v4.2](https://github.com/mongodb/mongo-tools/tree/v4.2) branch for critical changes.


Using the mongo-tools-common packages
---------------

To use the packages found here, use `mongo-tools` as a dependency instead.

First, add a `mongo-tools` dependency with the desired commit hash or tag in your `go.mod` file:
```
require github.com/mongodb/mongo-tools <commit-or-tag>
```

Then add the following import path with the desired subpackage in your Go file:
```
import "github.com/mongodb/mongo-tools/common/<subpackage>"
```


Making changes to mongo-tools/v4.2
---------------

If a change to `mongo-tools/v4.2` requires changes to `mongo-tools-common`, make the changes and create a pull request to merge them to `mongo-tools-common/v4.2`.

When merged, create a lightweight tag for `mongo-tools-common/v4.2` with appropriate minor/patch versions and push it:

```
git tag v2.X.x
git push upstream tag v2.X.x
```

Then in the `Gopkg.toml` file for `mongo-tools/v4.2`, update the `mongo-tools-common` constraint:

```
[[constraint]]
  name = "github.com/mongodb/mongo-tools-common"
  version = "v2.X.x"
```

Finally, revendor the dependency in `mongo-tools/v4.2` using `dep`:
```
dep ensure -update github.com/mongodb/mongo-tools-common
```
