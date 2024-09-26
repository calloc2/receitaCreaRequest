import threading
import tkinter as tk
from tkinter import ttk
from PIL import Image, ImageTk
import time
import requests
from config import load_env, get_db_params
from database import fetch_cnpj_list
from json_utils import load_cnpj_data, update_cnpj_data, save_cnpj_data

class CNPJApp:
    def __init__(self, root):
        self.root = root
        self.root.title("CREA-TO - Consulta CNPJ Para Receita Federal")
        self.root.geometry("1200x800")
        self.root.resizable(False, False)
        
        self.delay = tk.IntVar(value=20)
        self.running = False
        self.stop_event = threading.Event()
        self.last_checked_cnpj = tk.StringVar(value="Último CNPJ Consultado: Nenhum")
        
        self.style = ttk.Style()
        self.style.theme_use('clam')
        self.style.configure('TButton', background='#007acc', foreground='white', font=('Helvetica', 12))
        self.style.configure('TLabel', background='#e6f2ff', font=('Helvetica', 12))
        self.style.configure('TEntry', font=('Helvetica', 12))
        self.style.configure('TListbox', font=('Helvetica', 12))
        
        self.create_widgets()
        self.load_data()
        
    def create_widgets(self):
        self.bg_image = Image.open("assets/creatobg.png")
        self.bg_image = self.bg_image.resize((1200, 800), Image.LANCZOS)
        self.bg_photo = ImageTk.PhotoImage(self.bg_image)
        self.bg_label = tk.Label(self.root, image=self.bg_photo)
        self.bg_label.place(relwidth=1, relheight=1)
        
        self.start_button = ttk.Button(self.root, text="Iniciar", command=self.start_requests)
        self.start_button.place(x=20, y=320, width=100, height=30)
        
        self.stop_button = ttk.Button(self.root, text="Parar", command=self.stop_requests)
        self.stop_button.place(x=120, y=320, width=100, height=30)
        
        self.delay_label = ttk.Label(self.root, text="Atraso (segundos):")
        self.delay_label.place(x=20, y=280)
        
        self.delay_entry = ttk.Entry(self.root, textvariable=self.delay)
        self.delay_entry.place(x=155, y=280, width=100, height=30)
        
        self.listbox_frame = tk.Frame(self.root)
        self.listbox_frame.place(x=20, y=350, width=200, height=350)
        
        self.cnpj_listbox = tk.Listbox(self.listbox_frame, width=20, height=20, font=('Helvetica', 12))
        self.cnpj_listbox.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        
        self.scrollbar = tk.Scrollbar(self.listbox_frame, orient=tk.VERTICAL)
        self.scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        self.cnpj_listbox.config(yscrollcommand=self.scrollbar.set)
        self.scrollbar.config(command=self.cnpj_listbox.yview)
        
        self.last_checked_label = ttk.Label(self.root, textvariable=self.last_checked_cnpj)
        self.last_checked_label.place(x=20, y=740)
        
    def load_data(self):
        load_env()
        db_params = get_db_params()
        self.json_file = 'cnpj_list.json'
        
        cnpj_list = fetch_cnpj_list(db_params)
        self.cnpj_data = load_cnpj_data(self.json_file)
        self.cnpj_data = update_cnpj_data(cnpj_list, self.cnpj_data)
        save_cnpj_data(self.json_file, self.cnpj_data)
        
        self.update_listbox()
        
    def update_listbox(self):
        self.cnpj_listbox.delete(0, tk.END)
        for cnpj_code, data in self.cnpj_data.items():
            status = "✔" if data["requested"] else "✘"
            self.cnpj_listbox.insert(tk.END, f"{cnpj_code} {status}")
        
    def start_requests(self):
        if not self.running:
            self.running = True
            self.stop_event.clear()
            self.thread = threading.Thread(target=self.run_requests)
            self.thread.start()
        
    def stop_requests(self):
        self.running = False
        self.stop_event.set()
        if hasattr(self, 'thread'):
            self.thread.join(timeout=1)
        
    def run_requests(self):
        for cnpj_code, data in self.cnpj_data.items():
            if self.stop_event.is_set():
                break
            if data["requested"]:
                continue
            print(cnpj_code)
            url = f"https://receitaws.com.br/v1/cnpj/{cnpj_code}"
            print(url)
            response = requests.get(url)
            print(response.text)
            
            self.cnpj_data[cnpj_code]["requested"] = True
            save_cnpj_data(self.json_file, self.cnpj_data)
            self.update_listbox()
            
            self.last_checked_cnpj.set(f"Último CNPJ consultado: {cnpj_code}")
            
            time.sleep(self.delay.get())
        
def main():
    root = tk.Tk()
    root.configure(bg='#e6f2ff')
    icon_image = ImageTk.PhotoImage(file="assets/minerva.ico")
    root.iconphoto(False, icon_image)
    app = CNPJApp(root)
    root.mainloop()

if __name__ == "__main__":
    main()