# utility functions
import subprocess, time, numpy as np, datetime

def fast_pad(image, size, value=0): #faster than numpy.pad by a lot
    if type(size)==int: #if in assume same shape on all sizes
        size=((size,size),(size,size),(size,size))
    new_shape = (np.array(image.shape) + np.sum(size,axis=1)).astype(np.uint16)
    new_image = np.full(new_shape, value, dtype=image.dtype) if value else np.zeros(new_shape, dtype=image.dtype)
    original_slice = tuple(slice(size[tmp][0], new_image.shape[tmp]-size[tmp][1]) for tmp in range(image.ndim))
    new_image[original_slice] = image
    return(new_image)

def waitOutput(queueName,taskName='',waitInterval=10): # checks if cluster job is done
    while True:
        output = subprocess.check_output("bjobs -J "+queueName+" -noheader | wc -l", shell=True)
        if output==b'0\n':
            break
        else:
            extra = 'for '+taskName if taskName else ''
            print(str(datetime.datetime.now()).split('.')[0]+": working on",output.decode('ascii').strip(),"files",extra)
        time.sleep(waitInterval)