# BIBLIOTECAS RELEVANTES
import os
import sys
import subprocess
import warnings
warnings.filterwarnings('ignore')
import tkinter as tk
import customtkinter as ctk
from PIL import Image, ImageTk, ImageSequence

# Funções de Importação dos Scripts
def supply():
    import supply

def bind():
    import bind

def sku_activation():
    import sku_activation
    
def yield_first_deploy():
    import yield_first_deploy
    
def yield_second_deploy():
    import yield_second_deploy

def receipt():
    import receipt

def tax():
    import tax

def unconstrained_demand():
    import unconstrained_demand

def inventories():
    import inventories

def reposition_cost():
    import reposition_cost

def freight():
    import freight

def warehouses():
    import warehouses

def fixed_price():
    import fixed_price

def constrained_demand():
    import constrained_demand

# Funções para loading de informações
def help():
    cwd = os.getcwd()
    path = os.path.join(cwd, 'Guide.pdf')
    os.startfile(path)

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
        self.geometry("550x600")  # Increased height to accommodate the GIF
        self.title('ECFTO - VCM Data Preparation')
        
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
        
        sku_activation_bt = ctk.CTkButton(col1_frame, width=250, text="Definição Limites", command=sku_activation)
        sku_activation_bt.pack(pady=5, fill="x")
        
        unconstrained_demand_bt = ctk.CTkButton(col1_frame, width=250, text="Demanda Irrestrita", command=unconstrained_demand)
        unconstrained_demand_bt.pack(pady=5, fill="x")
        
        deploy_one_bt = ctk.CTkButton(col1_frame, width=250, text="1st Deploy: Prever SKUs da Demanda", command=yield_first_deploy)
        deploy_one_bt.pack(pady=5, fill="x")
        
        deploy_two_bt = ctk.CTkButton(col1_frame, width=250, text="2nd Deploy: Preencher Formulações", command=yield_second_deploy)
        deploy_two_bt.pack(pady=5, fill="x")
        
        supply_bt = ctk.CTkButton(col1_frame, width=250, text="Suprimento e Cap. Portuárias", command=supply)
        supply_bt.pack(pady=5, fill="x")
        
        inventories_bt = ctk.CTkButton(col1_frame, width=250, text="Estoque Inicial", command=inventories)
        inventories_bt.pack(pady=5, fill="x")
        
        rep_cost_bt = ctk.CTkButton(col1_frame, width=250, text="Custo de Reposição", command=reposition_cost)
        rep_cost_bt.pack(pady=5, fill="x")
        
        # Column 2 buttons
        col2_frame = ctk.CTkFrame(button_frame)
        col2_frame.grid(row=0, column=1, padx=5, pady=5, sticky="nsew")
        
        receipt_bt = ctk.CTkButton(col2_frame, width=250, text="Receitas de Movimentação", command=receipt)
        receipt_bt.pack(pady=5, fill="x")
        
        tax_bt = ctk.CTkButton(col2_frame, width=250, text="Atualização dos Impostos", command=tax)
        tax_bt.pack(pady=5, fill="x")
        
        freight_bt = ctk.CTkButton(col2_frame, width=250, text="Custos de Fretes", command=freight)
        freight_bt.pack(pady=5, fill="x")
        
        bind_bt = ctk.CTkButton(col2_frame, width=250, text="Amarração", command=bind)
        bind_bt.pack(pady=5, fill="x")
        
        warehouses_bt = ctk.CTkButton(col2_frame, width=250, text="Custos e Cap. de Armazenagem", command=warehouses)
        warehouses_bt.pack(pady=5, fill="x")
        
        prec_bt = ctk.CTkButton(col2_frame, width=250, text="Precificação", command=fixed_price)
        prec_bt.pack(pady=5, fill="x")
        
        cons_dem = ctk.CTkButton(col2_frame, width=250, text="Gerar Demanda Restrita", command=constrained_demand)
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

    def animate(self):
        """Update the GIF frame"""
        try:
            self.gif_label.configure(image=self.frames[self.current_frame])
            self.current_frame = (self.current_frame + 1) % len(self.frames)
            self.after(100, self.animate)  # Update every 100ms (adjust for speed)
        except:
            pass

app = App()
app.mainloop()