# isotropic XY upsample an n5 volume (assuming s2 label channel to s0 or s1)

import z5py, sys, os, time, tifffile, numpy as np,itertools
from common_func import waitOutput

input_name=sys.argv[1]
if not input_name.endswith('/'):
    input_name+='/'

scale='s2' #the other scales are obtained by downsampling
channel='label'

zsize=128 # must be multiple of z chunk size. takes awhile
xysize=1280

f=z5py.File(input_name)
shape=f[channel][scale].shape
chunks=f[channel][scale].chunks

up_scale=sys.argv[2] if len(sys.argv)>=3 else 's0'
if up_scale=='s0':
    up_factor=4
elif up_scale=='s1':
    up_factor=2
else:
    print('upsample scale must be s0 or s1')
    quit()

subgrids = np.ceil(shape/np.array((zsize,xysize,xysize))).astype(np.int)
up_shape=[shape[0],shape[1]*up_factor,shape[2]*up_factor] # only XY upsampling

label_channel='label'
try:
    group=f.create_group(label_channel)
except Exception as e:
    print(e)
    group=f[label_channel]

try:
    g=group.create_dataset(up_scale,shape=up_shape, chunks=chunks, dtype=np.uint32)
except Exception as e:
    print(e)
    g=group[up_scale]

if len(sys.argv)<=3:
    print(shape,chunks)
    for i,x in enumerate(itertools.product(*[range(sg) for sg in subgrids])):
        cmd='bsub -o /dev/null -n 3 -J up'+channel+name+'['+str(i+1)+'] python '+__file__+' '+input_name+' '+up_scale+' '+' '.join(map(str,[x[0]*zsize,(x[0]+1)*zsize,x[1]*xysize,(x[1]+1)*xysize,x[2]*xysize,(x[2]+1)*xysize]))
        print(cmd)
        os.system(cmd)
        time.sleep(0.05)
    waitOutput('up'+channel+name,'upsample')
else:
    z0=int(sys.argv[3])
    z1=int(sys.argv[4])
    y0=int(sys.argv[5])
    y1=int(sys.argv[6])
    x0=int(sys.argv[7])
    x1=int(sys.argv[8])
    zstack=list(range(z0,z1))
    nz=False
    newI=[]
    for i,I in enumerate(f[channel][scale][z0:z1,y0:y1,x0:x1]):
        if I.max():
            nz=True
        newI.append(np.repeat(np.repeat(I,up_factor,0),up_factor,1)) #upsample by duplication
        print('write',i)
    if nz:
        g[z0:z1,y0*up_factor:y1*up_factor,x0*up_factor:x1*up_factor]=newI
