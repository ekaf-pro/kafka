# Kafka Assignment Report
**Nama**: Eka Fajar Kharisma  
**Topic**: `events_topic` | **Partitions**: 3 | **Consumer Group**: `events_consumer_group`

---

## 1. Bagaimana Kafka Menentukan Partition Berdasarkan Key

Ketika producer mengirimkan event dengan key (misalnya `"user_1"`), Kafka menggunakan formula berikut untuk menentukan partition tujuan:

    partition = hash(key) % jumlah_partition

Kafka menggunakan algoritma **MurmurHash2** untuk menghitung hash dari key. Hasilnya di-modulo dengan jumlah partition yang ada di topic.

### Hasil Observasi dengan 3 Partition:

| Key     | Partition (hasil observasi) |
|---------|----------------------------|
| user_1  | Partition 0                |
| user_2  | Partition 1                |
| user_3  | Partition 0                |

Dari hasil running, `user_1` dan `user_3` sama-sama hash ke **Partition 0**, sedangkan `user_2` hash ke **Partition 1**. Partition 2 tidak mendapat event sama sekali karena tidak ada key yang hash ke sana.

**Sifat penting**: Key yang sama **selalu** masuk ke partition yang sama. Ini menjamin **ordering** — semua event dari `user_1` akan diproses secara berurutan.

### Mengapa Key-Based Partitioning Penting?
- **Ordering guarantee**: Event dari satu user selalu terurut
- **Locality**: Semua data satu user ada di satu partition
- **Predictable routing**: Producer bisa mengontrol distribusi data

---

## 2. Pengamatan Consumer Group

### Setup
- **Consumer Group**: `events_consumer_group`
- **Jumlah Consumer**: 2 (Consumer 1 & Consumer 2)
- **Jumlah Partition**: 3

### Apa yang Terjadi Ketika 2 Consumer dalam 1 Group?

Kafka secara otomatis melakukan **partition assignment** (rebalancing) ketika consumer bergabung ke group. Dengan 3 partition dan 2 consumer, distribusinya:

| Consumer   | Partition yang Ditangani |
|------------|--------------------------|
| Consumer 1 | Partition 0, Partition 1 |
| Consumer 2 | Partition 2              |

**Aturan utama**: Satu partition hanya bisa dikonsumsi oleh **satu consumer** dalam satu group pada satu waktu. Ini mencegah duplikasi pemrosesan.

### Pengamatan Distribusi Data (Hasil Aktual)

Dari hasil running program:

- **Consumer 1** menerima **semua event** dari partition 0 dan partition 1, yaitu event dari `user_1`, `user_2`, dan `user_3`
- **Consumer 2** tidak menerima event sama sekali karena mendapat assignment partition 2, namun tidak ada key yang hash ke partition 2
- Total **390 events** diproses oleh Consumer 1 dengan distribusi merata: `user_1: 130, user_2: 130, user_3: 130`
- Tidak ada event yang diproses dua kali

### Analisis

Fenomena ini terjadi karena hasil hash MurmurHash2 dari ketiga key (`user_1`, `user_2`, `user_3`) hanya jatuh ke partition 0 dan 1, tidak ada yang ke partition 2. Ini adalah perilaku normal Kafka — distribusi partition bergantung pada hasil hash key, bukan jumlah key.

### Kesimpulan

| Aspek | Penjelasan |
|-------|-----------|
| Load balancing | Partition dibagi antar consumer dalam group |
| No duplication | Satu partition → satu consumer dalam satu group |
| Fault tolerance | Consumer mati → partition otomatis di-reassign |
| Ordering | Event dalam satu partition tetap terurut |
| Hash dependency | Distribusi data bergantung hasil hash key, bukan jumlah key |

### Diagram Alur (Hasil Aktual)

    Producer
       |
       |-- key=user_1 --> Partition 0 --|
       |-- key=user_2 --> Partition 1 --|--> Consumer 1 (semua event)
       |-- key=user_3 --> Partition 0 --|

                        Partition 2 ------> Consumer 2 (tidak ada event)

                        (Consumer Group: events_consumer_group)
