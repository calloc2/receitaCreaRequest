import threading
import time
import requests
from PyQt5 import QtWidgets, QtGui, QtCore
from PIL import Image
from config import load_env, get_db_params
from database import fetch_cnpj_list
from json_utils import load_cnpj_data, update_cnpj_data, save_cnpj_data

class CNPJApp(QtWidgets.QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("CREA-TO - Consulta CNPJ Para Receita Federal")
        self.setGeometry(100, 100, 1200, 800)
        self.setFixedSize(1200, 800)
        
        self.delay = 20
        self.running = False
        self.stop_event = threading.Event()
        self.last_checked_cnpj = "Último CNPJ Consultado: Nenhum"
        
        self.init_ui()
        self.load_data()
        
    def init_ui(self):
        layout = QtWidgets.QVBoxLayout()
        
        self.bg_label = QtWidgets.QLabel(self)
        bg_image = Image.open("assets/creatobg.png")
        bg_image = bg_image.resize((1200, 800), Image.LANCZOS)
        bg_image.save("assets/creatobg_resized.png")
        self.bg_label.setPixmap(QtGui.QPixmap("assets/creatobg_resized.png"))
        self.bg_label.setScaledContents(True)
        layout.addWidget(self.bg_label)
        
        self.start_button = QtWidgets.QPushButton("Iniciar", self)
        self.start_button.setGeometry(20, 320, 100, 30)
        self.start_button.clicked.connect(self.start_requests)
        
        self.stop_button = QtWidgets.QPushButton("Parar", self)
        self.stop_button.setGeometry(120, 320, 100, 30)
        self.stop_button.clicked.connect(self.stop_requests)
        
        self.delay_label = QtWidgets.QLabel("Atraso (segundos):", self)
        self.delay_label.setGeometry(20, 280, 120, 30)
        
        self.delay_entry = QtWidgets.QSpinBox(self)
        self.delay_entry.setGeometry(155, 280, 100, 30)
        self.delay_entry.setValue(self.delay)
        
        self.listbox_frame = QtWidgets.QFrame(self)
        self.listbox_frame.setGeometry(20, 350, 200, 350)
        
        self.cnpj_listbox = QtWidgets.QListWidget(self.listbox_frame)
        self.cnpj_listbox.setGeometry(0, 0, 200, 350)
        
        self.last_checked_label = QtWidgets.QLabel(self.last_checked_cnpj, self)
        self.last_checked_label.setGeometry(20, 740, 300, 30)
        
        self.setLayout(layout)
        
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
        self.cnpj_listbox.clear()
        for cnpj_code, data in self.cnpj_data.items():
            status = "✔" if data["requested"] else "✘"
            self.cnpj_listbox.addItem(f"{cnpj_code} {status}")
        
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
            
            self.last_checked_cnpj = f"Último CNPJ consultado: {cnpj_code}"
            self.last_checked_label.setText(self.last_checked_cnpj)
            
            time.sleep(self.delay_entry.value())
        
def main():
    app = QtWidgets.QApplication([])
    window = CNPJApp()
    window.show()
    app.exec_()

if __name__ == "__main__":
    main()