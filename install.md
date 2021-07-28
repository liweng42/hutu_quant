## 系统自带了python3，安装pip3

``` bash
& python3 -V
& apt install -y python3-pip
& tar zxvf ./ta-lib-0.4.0-src.tar.gz
$ cd ta-lib
$ ./configure --prefix=/usr
$ make && make install
& pip3 install pandas matplotlib tushare redis Ta-Lib
& nohup python3 hutu_quant.py run_only_once &
```
