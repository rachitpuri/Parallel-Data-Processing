========================  Complile  ========================================
 
javac Assignment01.java

=========================  Run  ============================================

This program can be run in 2 ways:
1. Sequentially : java Assignment01 -input=DIR
2. Parallely : java Assignment01 -p -input=DIR

e.g. java Assignment01 -p -input="C:\Users\rachit\Desktop\all-v1\all\"

=========================   Solution   ======================================

Note: Header is considered while doing calculations.

corrupt line (K) : 60482
sanity test pass (F) : 12598804

Sorted list according to average price:

Airline | Mean | Median

F9 131.5885 91.55
WN 159.05132 136.0
AS 229.91948 191.5
MQ 288.11197 265.2
HA 302.44675 129.0
EV 305.5277 282.2
OO 307.18942 278.0
NK 487.36496 469.2
B6 523.50287 483.4
AA 530.0141 493.0
DL 598.65436 505.0
US 600.018 493.6
VX 652.63336 591.0
UA 968.93384 865.0

===========================  Conclusion  ======================================

When Program is run multithreaded then execution time is reduced to 48sec.
If run sequentially then execution time is somewhere around 200sec.
Hence, Multithreaded program runs faster than Sequential.

Data: https://s3.amazonaws.com/cs6240sp16/all-v1.tar