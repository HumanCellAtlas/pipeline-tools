# Stub submission WDL

This version of the submit.wdl doesn't actually do anything.

It has the same required inputs as the real submit wdl and we use it when we
want to test adapter WDLs without submitting anything at the end. It can be
helpful to exclude submission from certain tests because it can be time
consuming and depends on availability of an external system.
