class CustomFileManager:
    """
    A context manager to safely open and close a file,
    adding basic logging of the operation.
    """

    def __init__(self, filename, mode):
        self.filename = filename
        self.mode = mode
        self.file = None

    def __enter__(self):
        # 1. Setup/Resource Acquisition
        print(f"[{self.filename}]: Attempting to open file in '{self.mode}' mode...")
        self.file = open(self.filename, self.mode)

        # 2. Return the resource object (file handle) to the 'as' variable
        return self.file

    def __exit__(self, exc_type, exc_val, exc_tb):
        # 3. Cleanup/Resource Release (ALWAYS happens)
        if self.file:
            self.file.close()
            print(f"[{self.filename}]: File successfully closed.")

        # 4. Error Handling:
        if exc_type:
            # An exception occurred inside the 'with' block
            print(f"\n[{self.filename}]: An exception occurred!")
            print(f"Type: {exc_type.__name__}, Value: {exc_val}")

            # Returning True here would suppress the exception.
            # Returning False or None (default) allows the exception to propagate.
            return False

            # No exception occurred
        return False  # Or None (default)


# --- Usage ---
try:
    with CustomFileManager('test_log.txt', 'w') as f:
        print(f"[{f.name}]: Writing data...")
        f.write("This line is written inside the context.")
        # f.does_not_exist() # Uncomment this to see exception handling
        print(f"[{f.name}]: Finished writing.")


except Exception as e:
    print(f"\nCaught the propagated exception: {e}")