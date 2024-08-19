import os
from concurrent.futures import ThreadPoolExecutor

# Menghitung jumlah CPU yang tersedia dan max_workers
cpu_count = 2
max_workers = min(32, cpu_count * 5)

# Fungsi sederhana untuk tugas-tugas yang akan dijalankan oleh thread
def worker_task(task_id):
    print(f"Tugas {task_id} sedang dijalankan.")

# Menggunakan ThreadPoolExecutor dengan max_workers yang dihitung
with ThreadPoolExecutor(max_workers=max_workers) as executor:
    for i in range(50):  # Misalnya, ada 50 tugas yang harus dikerjakan
        executor.submit(worker_task, i)

print(f"ThreadPoolExecutor berjalan dengan {max_workers} worker.")
