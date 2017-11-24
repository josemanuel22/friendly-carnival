#!/bin/bash

# Current time in milliseconds
now=1505988635
metric=prueba2
value=42
host=192.168.1.13

echo "put $metric $now $value host=A" | nc -w 30 $host 4242
