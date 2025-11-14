import tkinter as tk
from tkinter import messagebox

try:
    print("Attempting to create Tk root window...")
    root = tk.Tk()
    print("Tk root window created successfully.")
    root.title("Test")
    label = tk.Label(root, text="Hello, Tkinter!")
    label.pack(pady=20, padx=20)
    print("Starting mainloop...")
    root.mainloop()
    print("Mainloop finished.")
except Exception as e:
    print(f"An error occurred: {e}")
    messagebox.showerror("Tkinter Test Error", f"An error occurred: {e}")
