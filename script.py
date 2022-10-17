import frame_fixtures as ff

from static_frame import Quilt

f1 = ff.parse("s(4,4)|v(int,float)|c(I,str)").rename("f1")
f2 = ff.parse("s(4,4)|v(str)|c(I,str)").rename("f2")
f3 = ff.parse("s(4,4)|v(bool)|c(I,str)").rename("f3")

q = Quilt.from_frames((f1, f2, f3), include_index=False, retain_labels=False, axis=0)

a = q._extract(None)
b = q._extract(0)
c = q._extract([0])
d = q._extract(None, 0)
e = q._extract(None, [0])
f = q._extract(0, 0)
g = q._extract(0, [0])
h = q._extract([0], 0)
i = q._extract([0], [0])
j = q._extract_array(None)
k = q._extract_array(0)
l = q._extract_array([0])
m = q._extract_array(None, 0)
n = q._extract_array(None, [0])
o = q._extract_array(0, 0)
p = q._extract_array(0, [0])
r = q._extract_array([0], 0)
s = q._extract_array([0], [0])
