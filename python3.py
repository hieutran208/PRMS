from math import *
x = int(input('Nhập số nguyên cần kiểm tra: '))
y = x + 15
if gcd(x,y) >= 10:
    print("ƯCLN của %s và %s lớn hơn 10" %(x, y))
elif gcd(x,y) >= 5:
    print("ƯCLN của %s và %s lớn hơn 5, nhỏ hơn 10" %(x, y))
else:
    print("ƯCLN của %s và %s nhỏ hơn 5" %(x, y))

# Toán tử ba ngôi
str = '30 < 45' if x < y else '30 > 45'
print(str)

# Vong lap for
S1 = 0
for i in range(0,100,2):
    S1 = S1 + i
    if i == 10: 
        break # Dừng vòng lặp
print(S1)

# Vòng lặp while
n = 0
S2 = 0
while n <= 20:
    S2 = S2 + n
    n += 1
print(S2)