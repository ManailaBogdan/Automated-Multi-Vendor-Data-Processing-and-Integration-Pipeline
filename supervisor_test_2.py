import time, sys, os, string, random

def main():
    
    
    while True:
        l = random.choice(string.ascii_letters)
        print(l)
        sys.stdout.flush()
        time.sleep(1)
        
        
if __name__ == "__main__":
    main()