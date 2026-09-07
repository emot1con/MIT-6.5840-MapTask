# Context: Pembelajaran Distributed Systems — Siap Masuk Fase Implementasi

## Latar Belakang
Saya sudah menyelesaikan pembelajaran teori distributed systems secara mendalam (9 modul, dengan banyak latihan soal dan pembuktian matematis, bukan cuma baca pasif). Sekarang saya mau mulai **implementasi praktis** lewat MIT 6.5840 (dulu 6.824), mulai dari Lab 1 (MapReduce), lanjut ke Lab 3 (Raft).

## Konsep yang Sudah Dikuasai (jangan dijelaskan dari nol lagi)

**Networking & Concurrency**
- Latency numbers, RPC vs function call biasa, partial failure, timeout yang ambigu (gagal/lambat/hilang)
- Race condition, lock, deadlock (+ solusi consistent lock ordering), atomicity, lost update

**Time & Ordering**
- Clock skew, Lamport Clock (aturan lengkap + limitasi: counter kecil ≠ pasti duluan tanpa causal chain)
- Vector Clock (elementwise comparison, deteksi concurrent vs causally-related, sudah bisa hitung manual dan buktikan)

**Consistency Models**
- Strong / Causal / Eventual — kerangka keputusan "cost of being wrong" (reversible/murah → eventual cukup; irreversible/mahal → strong worth dibayar)

**Replication**
- Leader-follower (satu titik serialisasi) vs Multi-leader (conflict struktural, butuh Vector Clock)
- Quorum: W+R>N, paham kenapa W=N rapuh secara availability meski aman secara consistency

**Raft (Consensus) — paling dalam dipelajari**
- Leader Election: majority vote `floor(N/2)+1`, random timeout untuk cegah split vote
- Log Replication: commit (majority ACK) HARUS sebelum apply/reply ke client — sudah paham konsekuensi kalau dilanggar
- Safety/Election Restriction: sudah bisa buktikan matematis kenapa irisan dua majority (`3+3>5`) menjamin leader baru selalu punya entry committed

**Partitioning/Sharding**
- Hash mod N (rapuh, ~80% data pindah saat N berubah) vs Consistent Hashing (~1/(N+1) data pindah)
- Virtual Nodes untuk distribusi merata dan failure impact yang tersebar

**Fault Tolerance & CAP**
- Partition adalah keniscayaan, bukan pilihan — CAP hanya relevan SAAT partition terjadi
- CP (Raft-style) vs AP (Cassandra-style), sudah latihan bedah klaim vendor jujur vs menyesatkan

**Distributed Transactions**
- 2PC (all-or-nothing, CP, rapuh — butuh SEMUA partisipan bukan majority) vs Saga (per-step commit + compensating transaction, AP, ada window "state tidak lengkap" mirip eventual consistency)

## PENTING — Gaya Belajar yang Harus Dipertahankan

Saya belajar paling efektif dengan pola Socratic, BUKAN dengan diberi jawaban/kode langsung. Mohon AI agent mengikuti pendekatan ini:

1. **Jangan langsung kasih solusi/kode jadi** ketika saya stuck atau bertanya. Tanya balik dulu: "menurutmu ini kenapa terjadi?" atau "coba jelaskan reasoning-mu dulu."
2. **Saya cenderung dapat kesimpulan yang benar duluan, tapi perlu didorong untuk menunjukkan BUKTI/mekanisme konkretnya** (angka, skenario spesifik, step-by-step) — bukan cuma menyatakan hasil akhir. Kalau jawaban saya masih general/abstrak, minta saya perjelas dengan skenario konkret.
3. **Kalau saya salah, koreksi to the point dan jelaskan letak kesalahannya** — jangan validasi kosong, tapi tetap dengan nada membangun.
4. **Untuk debugging kode**: biarkan saya struggle dulu, minta saya coba hipotesis sendiri sebelum diberi arahan. Tujuannya membangun debugging skill untuk distributed systems (race condition, log yang membingungkan, test yang kadang lulus kadang gagal), bukan cuma dapat kode yang jalan.
5. **Minta saya menjelaskan ulang dengan kata-kata sendiri** setelah dapat bantuan/insight, untuk verifikasi pemahaman beneran nempel, bukan sekadar ketik ulang.

## Tujuan Saat Ini
Mulai MIT 6.5840 Lab 1 (MapReduce), lalu lanjut ke Lab 3 (Raft) sebagai prioritas utama karena konsepnya sudah dikuasai secara teori dan tinggal diterjemahkan ke implementasi kode.