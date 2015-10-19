# kafka_irc
IRC implementation with Apache Kafka

## Oleh : 
Rita Sarah / 13512009 
Andarias Silvanus / 13512022

## Petunjuk instalasi
File yang kami upload merupakan project NetBeans

    Nyalakan server zookeeper dan broker kafka 
    Buka project NetBeans tersebut di NetBeans
    Run Client.java

## Tes yang dilakukan 
1. tes yang kami lakukan dengan menggunakan 2 client 
2. mengganti nickname dengan /NICK dan join dengan channel tertentu dengan /JOIN 
3. Client pertama melakukan /JOIN pat_channel , /JOIN pat_channel2 
4. Client kedua melakukan /JOIN pat_channel 
5. Client pertama mengirimkan broadcast "test" dan menerima pesan itu dua kali dari channel pat_channel dan pat_channel2 
6. Client kedua mendapatkan pesan itu dari channel pat_channel 
7. Client pertama mengganti nick dengan /NICK RitaSarah 
8. Client pertama mengirimkan pesan @pat_channel test personal 
9. Client kedua mendapatkan pesan test personal dari channel pat_channel 
10. Client kedua melakukan /LEAVE pat_channel 
11. Client pertama mengirimkan pesan @pat_channel test leave 
12. Client kedua tidak mendapatkan pesan test di channel pat_channel

Hasil pengujian untuk semua butir diatas berhasil
