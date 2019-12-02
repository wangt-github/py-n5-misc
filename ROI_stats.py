## ROI statistics from export TIFF output

import sys,numpy as np, csv,os

bin_size = 1000
maxbin = bin_size*500

d=sys.argv[1] # directory
vn=sys.argv[2] # volume name
if not d.endswith('/'):
    d+='/'

counts={}
for x in os.listdir(d):
    if x.endswith('.txt'):
        with open(d+x) as f:
            r=csv.reader(f,dialect='excel-tab')
            for i in r:
                c=int(float(i[0]))
                if c not in counts:
                    counts[c]=0
                counts[c]+=int(float(i[1]))

with open(vn+'.counts_by_ID.txt','w',newline='') as g: # file name from export TIFF
    w=csv.writer(g,dialect='excel-tab')
    w.writerows(sorted(counts.items()))

c,b=np.histogram(sorted(counts.values())[:-1],list(range(0,maxbin, bin_size)))
with open(vn+'.count_hist.txt','w',newline='') as g:
    w=csv.writer(g,dialect='excel-tab')
    w.writerows(np.array((b[:-1],c)).T)

