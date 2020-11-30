from pci.chdetop import chdetop
from pci.expolras import expolras
from pci.areareport import areareport
from pci.map import map as bit_encode
from pci.pcimod import pcimod
from pci.poly2bit import poly2bit
from pci.nspio import Report, enableDefaultReport
import os

working_dir= r'D:\data'
input_dir = os.path.join(working_dir, "infiles")
change_image= os.path.join(input_dir, "changendvi.pix")


print('Create change detection raster')
chdetop(fili=os.path.join(input_dir,"currentndvi.pix"),
        dbic=[1,],
        fili_ref=os.path.join(input_dir,"prevndvi.pix"),
        dbic_ref=[1],
        filo=change_image,
        algo="INTENRATIO")

print('Extract change as polygons')
expolras(fili=change_image,
         dbic=[4],
         thrtype="PixelValue",
         tval=[90],
         areaval=[100],
         filo=change_image)

print('Convert the polygons to a bitmap')
poly2bit(fili=change_image,
         dbvs=[2],
         filo=change_image)

print('Add a new channel')
pcimod(file=change_image,
       pciop="ADD",
       pcival=[1,0,0,0])

print('Encode bitmap into an empty image channel')
bit_encode(file=change_image,
           dbib=[3],
           valu=[1],
           dboc=[5])

        