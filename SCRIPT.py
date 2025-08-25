# BIBLIOTECAS RELEVANTES
import os
import sys
import subprocess
import webbrowser
import warnings
warnings.filterwarnings('ignore')
import tkinter as tk
import customtkinter as ctk
from PIL import Image, ImageTk, ImageSequence

# Funções de Importação dos Scripts
def supply():
    import supply # OK

def bind():
    import bind # OK

def sku_activation():
    import sku_activation # OK
    
def yield_deploy():
    import yield_deploy # OK
    
def receipt():
    import receipt # OK

def tax():
    import tax # OK

def unconstrained_demand():
    import unconstrained_demand # OK

def inventories():
    import inventories # OK

def reposition_cost():
    import reposition_cost # OK

def freight():
    import freight # OK

def warehouses():
    import warehouses

def fixed_price():
    import fixed_price # OK

def constrained_demand():
    import constrained_demand

def limits():
    import limits # OK

# Funções para loading de informações
def help():
    cwd = os.getcwd()
    path = os.path.join(cwd, 'Guide.pdf')
    os.startfile(path)

def abrir_git():
    webbrowser.open("https://github.com/murilo-tsu/VCM")

def changelog():
    subprocess.call(['notepad.exe','Changelog.txt'])

def open_console():
    subprocess.call(['cmd.exe'])

def combine_funcs(*funcs):
    def combined_func(*args, **kwargs):
        for f in funcs:
            f(*args, **kwargs)
    return combined_func

class App(ctk.CTk):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.geometry("600x600")  # Increased height to accommodate the GIF
        self.title('Data Preparation App')
        
        # GIF Animation Setup
        img_path = os.path.join('images', 'A realidade.gif')
        try:
            self.gif = Image.open(img_path)
            self.frames = []
            for frame in ImageSequence.Iterator(self.gif):
                frame = frame.resize((200, 200))
                self.frames.append(ImageTk.PhotoImage(frame))
            
            self.current_frame = 0
            self.gif_label = tk.Label(self)  # Using standard tkinter Label for animation
            self.gif_label.pack(pady=10)
            self.animate()
        except Exception as e:
            print(f"Error loading GIF: {e}")
            # Fallback to static image if GIF fails
            try:
                static_img = ctk.CTkImage(light_image=Image.open(os.path.join('images', 'icon.png')), size=(150, 150))
                self.gif_label = ctk.CTkLabel(self, image=static_img, text="")
                self.gif_label.pack(pady=10)
            except:
                pass
        
        self.wm_iconbitmap(os.path.join('images', 'icon.ico'))
    
        # Main container frame
        main_frame = ctk.CTkFrame(self)
        main_frame.pack(pady=10, padx=10, fill="both", expand=True)
        
        jobs = ctk.CTkLabel(main_frame, text="Selecione o script a ser executado: ")
        jobs.pack(pady=5)
        
        # Create a frame for the button grid
        button_frame = ctk.CTkFrame(main_frame)
        button_frame.pack(pady=10, padx=10, fill="both", expand=True)
        
        # Column 1 buttons
        col1_frame = ctk.CTkFrame(button_frame)
        col1_frame.grid(row=0, column=0, padx=5, pady=5, sticky="nsew")
        
        sku_activation_bt = ctk.CTkButton(col1_frame, width=250, text="[1] Ativação SKU por correntes", command=sku_activation)
        sku_activation_bt.pack(pady=5, fill="x")
        
        deploy_one_bt = ctk.CTkButton(col1_frame, width=250, text="[2] Deploy: Gerar Listas Técnicas", command=yield_deploy)
        deploy_one_bt.pack(pady=5, fill="x")
        
        unconstrained_demand_bt = ctk.CTkButton(col1_frame, width=250, text="[3] Demanda Irrestrita", command=unconstrained_demand)
        unconstrained_demand_bt.pack(pady=5, fill="x")
                      
        supply_bt = ctk.CTkButton(col1_frame, width=250, text="[4] Suprimento e Cap. Portuárias", command=supply)
        supply_bt.pack(pady=5, fill="x")

        bind_bt = ctk.CTkButton(col1_frame, width=250, text="[5] Amarração", command=bind)
        bind_bt.pack(pady=5, fill="x")
        
        inventories_bt = ctk.CTkButton(col1_frame, width=250, text="[6] Estoque Inicial", command=inventories)
        inventories_bt.pack(pady=5, fill="x")
        
        rep_cost_bt = ctk.CTkButton(col1_frame, width=250, text="[7] Custo de Reposição", command=reposition_cost)
        rep_cost_bt.pack(pady=5, fill="x")
        
        # Column 2 buttons
        col2_frame = ctk.CTkFrame(button_frame)
        col2_frame.grid(row=0, column=1, padx=5, pady=5, sticky="nsew")
        
        receipt_bt = ctk.CTkButton(col2_frame, width=250, text="[8] Receitas de Movimentação", command=receipt)
        receipt_bt.pack(pady=5, fill="x")
        
        tax_bt = ctk.CTkButton(col2_frame, width=250, text="[9] Atualização dos Impostos", command=tax)
        tax_bt.pack(pady=5, fill="x")
        
        freight_bt = ctk.CTkButton(col2_frame, width=250, text="[10] Custos de Fretes", command=freight)
        freight_bt.pack(pady=5, fill="x")
         
        warehouses_bt = ctk.CTkButton(col2_frame, width=250, text="[11] Capacidade e Custo Armazenagem/Handling", command=warehouses)
        warehouses_bt.pack(pady=5, fill="x")
        
        prec_bt = ctk.CTkButton(col2_frame, width=250, text="[12] Preço Fixo Produtos", command=fixed_price)
        prec_bt.pack(pady=5, fill="x")
        
        lim_bt = ctk.CTkButton(col2_frame, width=250, text="[13] Limites: Descarga e Produção", command=fixed_price)
        lim_bt.pack(pady=5, fill="x")
        
        cons_dem = ctk.CTkButton(col2_frame, width=250, text="[14] Gerar Demanda Restrita", command=constrained_demand)
        cons_dem.pack(pady=5, fill="x")
        
        # Configure grid weights to make columns expand equally
        button_frame.grid_columnconfigure(0, weight=1)
        button_frame.grid_columnconfigure(1, weight=1)

        # Help buttons at the bottom
        help_frame = ctk.CTkFrame(main_frame)
        help_frame.pack(pady=5, fill="x")

        hf = ctk.CTkLabel(help_frame, text="Links úteis: ")
        hf.pack(pady=5)

        help_bt = ctk.CTkButton(help_frame, text="Manual do Usuário", command=help)
        help_bt.pack(side="left", padx=5)
        
        git_bt = ctk.CTkButton(help_frame, text="Git Hub Page", command=abrir_git)
        git_bt.pack(side="left", padx=5)

    def animate(self):
        try:
            self.gif_label.configure(image=self.frames[self.current_frame])
            self.current_frame = (self.current_frame + 1) % len(self.frames)
            self.after(100, self.animate)
        except:
            pass

app = App()
app.mainloop()