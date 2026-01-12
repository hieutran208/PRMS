from math import *
from fractions import *
a = 1.6
b = 2
c = 7
class ErrorAge(Exception):
    pass
print(f"Giá trị của {a} + {b} = {a+b}")
print(type(a))
print(Fraction(1,b))
print(fabs(a))
z = 3 + 4j
print(z, " ", z.real, " ", z.imag)
str_a = 'My name is Hieu\a'
print (len(str_a) - 1)
print('My name is %s'%(a))
print(c%a)
print(c**b)
print(c//a)
print(('a' in 'abcd') or (1 != 1))
print(sqrt(36))
print(ceil(pow(c,a)))
print(gcd(b,c))
try:
    d = int(input("Nhập d: "))
    if d < 0: 
        raise ErrorAge('Tuổi không được âm')
except ErrorAge as e:
    print(f'Lỗi: {e}')
finally:
    print("Đã hoàn thành bước nhập tuổi")
