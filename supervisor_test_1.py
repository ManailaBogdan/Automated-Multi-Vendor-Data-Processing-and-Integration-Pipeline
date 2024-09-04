import time, sys, os

def main():
    
    t = 0
    while True:
        t += 1
        print(t)
        sys.stdout.flush()
        time.sleep(1)
        
        
if __name__ == "__main__":
    main()