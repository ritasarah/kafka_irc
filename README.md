# kafka_irc
IRC implementation with Apache Kafka

## Oleh : 
Rita Sarah / 13512009 
Andarias Silvanus / 13512022

## Petunjuk instalasi
File yang kami upload merupakan project NetBeans

    1. Nyalakan server zookeeper dan broker kafka
	command utk menyalakan zookeeper: bin/zookeeper-server-start.sh config/zookeeper.properties
	command utk menyalakan broker: bin/kafka-server-start.sh config/server.properties
    2. Buka project NetBeans tersebut di NetBeans
    3. Run Client.java

## Tes yang dilakukan 
1. tes yang kami lakukan dengan menggunakan 2 client 
2. mengganti nickname dengan /NICK dan join dengan channel tertentu dengan /JOIN 
3. Client pertama melakukan /JOIN PatChannel , /JOIN PatChannel2 
4. Client kedua mengganti nickname dengan nickname yang sama dengan client pertama. Hal ini gagal karena nickname tersebut telah diambil.
5. Client kedua melakukan /JOIN PatChannel
6. Client pertama mengirimkan broadcast "test" dan menerima pesan itu dua kali dari channel PatChannel dan PatChannel2 
7. Client kedua mendapatkan pesan itu dari channel PatChannel
8. Client pertama mengirimkan pesan @PatChannel test personal 
9. Client kedua mendapatkan pesan test personal dari channel pat_channel 
10. Client kedua melakukan /LEAVE PatChannel
11. Client pertama mengirimkan pesan @PatChannel test leave 
12. Client kedua tidak mendapatkan pesan test di channel PatChannel
13. Client kedua mengirimkan pesan pada @PatChannel. Pesan tidak terkirim karena client sudah meninggalkan channel tersebut

Hasil pengujian untuk semua butir diatas berhasil
