# Sample Queries

For energy efficient stream processing project

## Usage

### tsclog

put tsclog library into `resources` before compiling jar

```
git clone git@github.com:jappavoo/tsclog.git
cd tsclog
make
cp tsclog/libtsclog.so ./src/main/resources/libtsclog.so
```

### compile

`mvn clean package`

## Queries

see https://github.com/CASP-Systems-BU/cloud-provider-benchmarks

### Query1: map + filter + sink

Input parameter: event generating rate.  ```--ratelist [<rate>_<duration>]^n```

the duration is in **milliseconds**

e.g. ```--ratelist 250_300000_11000_300000```

### Query1tsc

Query1 with tsclog

### Query3: a stateful record-at-a-time two-input operator (incremental join) 

contains 2 sources: auctions and persons

![image](https://user-images.githubusercontent.com/7352163/144359118-dd0fd056-d270-4c54-b8fc-40adba3539c7.png)

Input parameter: event generating rate.  ```--ratelist [<auctions_rate>_<duration>_<persons_rate>_<duration> ]^n```

e.g. ```--ratelist 50000_300000_10000_300000_1000_600000_200_600000```

### Query5: sliding window

![image](https://user-images.githubusercontent.com/7352163/144932007-2109feff-f978-4b04-a811-08ccb121547c.png)

Input parameter: event generating rate.  ```--ratelist [<rate>_<duration> ]^n```

e.g. ```--ratelist 250_300000_11000_300000```

### Query8: tumbling window join

![image](https://user-images.githubusercontent.com/7352163/144933551-a0582476-9cbd-410c-8265-4b0e9d6946d3.png)

Input parameter: event generating rate.  ```--ratelist [<auctions_rate>_<duration>_<persons_rate>_<duration> ]^n```

e.g. ```--ratelist 50000_300000_10000_300000_1000_600000_200_600000```


