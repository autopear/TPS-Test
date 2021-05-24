#!/bin/bash
g++ -std=c++11 -W -Wno-unused-result -Wmaybe-uninitialized -O3 \
    main_write.cpp file_write.cpp \
    -o file_write 

g++ -std=c++11 -W -Wno-unused-result -Wmaybe-uninitialized -pthread -O3 \
    main_lookup.cpp file_lookup.cpp \
    -o file_lookup 

g++ -std=c++11 -W -Wno-unused-result -Wmaybe-uninitialized -pthread -O3 \
    main_scan.cpp file_scan.cpp \
    -o file_scan 
