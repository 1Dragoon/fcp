# fcp

Multi-threaded recursive file copy, now available in rust! The directory scan is multi-threaded, and each individual file is copied in its own thread

Simply run it like so:

```bash
fcp <sourcedir> <destdir>
```

No error handling of any kind is currently implemented, so it will simply crash if any problems are encountered
