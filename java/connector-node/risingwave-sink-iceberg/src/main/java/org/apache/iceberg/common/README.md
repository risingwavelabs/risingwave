# Why we need this package?

In this package we have override the `iceberg-common` package, since in original `iceberg-common` it uses `Thread.getContextClassLoader` to load classes dynamically.
While this works well in most cases, it will fail when invoked by jni, since by default jni threads was passed bootstrap class loader, and `Thread.getContextClassLoader`
will inherit parent thread's class loader. That's to say, all threads created by jni will use bootstrap class loader. While we can use `Thread.setContextClassLoader` to it system class loader
manually, but it's not possible in all cases since iceberg used thread pools internally, which can't be hooked by us.