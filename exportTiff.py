# export an n5 volume into individual TIFFs
# can output counts for statistics (more suitable for label values)
# can also re-map output labels

import z5py, sys, os, time, tifffile, numpy as np, math
from common_func import waitOutput

input_name=sys.argv[1]
if not input_name.endswith('/'):
    input_name+='/'

output_name=input_name.split('/')
name=output_name[-2]
output_name='/'.join(output_name[:-2]+[output_name[-2]+'_tiff','']) # append '_copy' to input directory name

channel=sys.argv[2]
scale=sys.argv[3] if len(sys.argv)>3 and sys.argv[3].startswith('s') else 's0'

zsize=16 # makes sense to do something divisble by 128 because that is typically Z chunk size
pixelCounts=False
remap=False

f=z5py.File(input_name)
output_name+=channel+'/'+scale+'/'
try:
    os.makedirs(output_name, exist_ok=True)
    print('making',output_name)
except:
    pass

if len(sys.argv)<=4:
    shape=f[channel][scale].shape
    tot_mem=shape[1]*shape[2]*2*zsize/1E9+8
    node=math.ceil(tot_mem/15)
    for i in range(0,shape[0],zsize):
        cmd='bsub -o /dev/null -J export'+channel+name+'['+str(i+1)+'] -n '+str(node)+' python '+__file__+' '+input_name+' '+channel+' '+scale+' '+str(i)+' '+str(i+zsize)
        print(cmd)
        os.system(cmd)
        time.sleep(0.05)
    waitOutput('export'+channel+name,'exporting')
    if pixelCounts:
        cmd='python ROI_stats.py '+output_name+' '+name
        print(cmd)
        os.system(cmd)
else:
    z0=int(sys.argv[4])
    z1=int(sys.argv[5])
    zstack=list(range(z0,z1))
    
    if remap: #if we want to re-assign values (for labels)
        value_map=dict(zip(ID_in,ID_out)) # two lists of IDs
        I_map=np.vectorize(value_map.get)
    for i,I in enumerate(f[channel][scale][z0:z1,:,:]): #could make limits here on X and Y
        fn=output_name+channel+'z'+str(zstack[i]).zfill(5)
        if remap:
            tifffile.imwrite(fn+'.remapped.tif',I_map(I).astype(np.uint16))
        tifffile.imwrite(fn+'.tif',I)
        if pixelCounts:
            np.savetxt(fn+'.txt',np.array(np.unique(I, return_counts=True)).T,delimiter='\t')