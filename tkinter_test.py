import tkinter as tk

print("DEBUG: tkinter_test.py a iniciar.")

try:
    print("DEBUG: Criando a janela raiz Tk...")
    root = tk.Tk()
    print("DEBUG: Janela raiz Tk criada.")

    root.title("Teste Tkinter")
    root.geometry("200x100")

    label = tk.Label(root, text="Olá, Tkinter!")
    label.pack(pady=20)

    print("DEBUG: Widgets criados. A iniciar o mainloop em 2 segundos...")

    # Sair automaticamente após 2 segundos
    root.after(2000, root.destroy)

    root.mainloop()

    print("DEBUG: tkinter_test.py concluído com sucesso.")

except Exception as e:
    print(f"ERRO: Ocorreu uma exceção: {e}")
