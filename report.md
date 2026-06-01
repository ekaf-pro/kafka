# Kafka Assignment Report
**Nama**: Eka Fajar Kharisma  
**Topic**: `events_topic` | **Partitions**: 3 | **Consumer Group**: `events_consumer_group`

---

## 1. Bagaimana Kafka Menentukan Partition Berdasarkan Key

Ketika producer mengirimkan event dengan key (misalnya `"user_1"`), Kafka menggunakan formula berikut untuk menentukan partition tujuan:

```
partition = hash(key) % jumlah_partition
```

Kafka menggunakan algoritma **MurmurHash2** untuk menghitung hash dari key. Hasilnya di-modulo dengan jumlah partition yang ada di topic.

### Contoh dengan 3 Partition:

| Key     | Hash (MurmurHash2) | Partition |
|---------|-------------------|-----------|
| user_1  | hash("user_1") % 3 | → tetap sama setiap kali |
| user_2  | hash("user_2") % 3 | → tetap sama setiap kali |
| user_3  | hash("user_3") % 3 | → tetap sama setiap kali |

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

### Pengamatan Distribusi Data

Dari hasil running program:

- **Consumer 1** hanya menerima event dari partition yang ditugaskan kepadanya (misal partition 0 & 1), yang berarti event dari `user_1` dan `user_2`
- **Consumer 2** hanya menerima event dari partition yang ditugaskan (misal partition 2), yang berarti event dari `user_3`
- Tidak ada event yang diproses dua kali oleh kedua consumer
- Ketika salah satu consumer di-stop (Ctrl+C), Kafka otomatis **rebalance** — partition yang tadinya ditangani consumer yang mati diambil alih oleh consumer yang masih hidup

### Kesimpulan

| Aspek | Penjelasan |
|-------|-----------|
| Load balancing | Partition dibagi rata antar consumer dalam group |
| No duplication | Satu partition → satu consumer dalam satu group |
| Fault tolerance | Consumer mati → partition otomatis di-reassign |
| Ordering | Event dalam satu partition tetap terurut |

### Diagram Alur

```
Producer
   │
   ├── key=user_1 ──→ Partition 0 ──→ Consumer 1
   ├── key=user_2 ──→ Partition 1 ──→ Consumer 1
   └── key=user_3 ──→ Partition 2 ──→ Consumer 2
                         (Consumer Group: events_consumer_group)
```
