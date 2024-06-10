import math

values = []
eps = []

with open("metrics.txt", "a+") as f:
    f.seek(0)
    for line in f.readlines():
        req = line.strip().split()
        values.append(float(req[-2]))
        eps.append(300/float(req[-2]))

    average = round(sum(values)/len(values), 4)
    eps_avg = round(sum(eps)/len(eps), 2)

    f.write(f"Average time taken per batch: {average} s\nAverage entries per second: {eps_avg}\n")

