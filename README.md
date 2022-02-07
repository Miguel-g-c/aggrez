# `aggrez`
`aggrez` is a command line tool that takes an openLCA database and replace
the unchanged background processes for LCI results.

The tool takes the following arguments:
- `-src`: An openLCA database backup file (`zolca` file) as source database
to aggregate. Required.
- `-dest`: An openLCA database backup file name for the resulting database. 
Required.
- `-from`: A date using the `dd.MM.yyyy` format. Every process in the source
database newer than this date will not be aggregated. Optional (if not 
provided, every process will be aggregated).
- `-threads`: Number of threads to be used in the aggregation. Optional (if 
not provided, the tool will use 2 threads).

In order to run the tool you need to have Java 17+ installed. If this is 
the case, you can run it in the following way:

```bash
java -jar aggrez.jar -src input.zolca -dest target.zolca -from 27.11.2021 -threads 16
```
