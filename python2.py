# List
lst_a = [3, 2, 1, 4]
for i in lst_a:
    print(i)  
lst_a.append(3) # thêm cuối  
lst_a.insert(2, 'Hiếu') # chèn vào vị trí 1  
#lst_a.remove(lst_a[2]) # xóa theo giá trị  hoặc del(lst_a[2])
del(lst_a[2])
print (lst_a)
lst_a.sort(reverse=True)
print (lst_a)
lst_a.pop() # xóa cuối  
print(sum(lst_a) / len(lst_a))

# Tuple
tup_a = ("apple", "banana", "orange")
tup_b = ("a@gmail.com", "b@gmail.com", "a@gmail.com")
tup_a = tup_a + tup_b
print(tup_a)

# Set
set_a = {1,2,5,8} # set_a = {1,2,5,8,8} --> phần tử 8 trùng sẽ bị loại bỏ tự động vì set không cho trùng
set_b = {1,2,'Hiếu'}
print(max(set_a))
if 8 in set_a:
    set_a.remove(8)
    set_b.add(4)
print("set_a hop set_b:", set_a | set_b) 
print("set_a giao voi set_b:", set_a & set_b)
print(set_a)
print(set_b)
emails = ["a@gmail.com", "b@gmail.com", "a@gmail.com"]  
unique_emails = set(emails)  # Chuyển sang kiểu set
print(unique_emails)  
# Dictionary
dic_a = { "math": 8, "english": 7 } 
dic_a["physics"] = 9
dic_a.pop("english")
print(dic_a["math"]) 
print(sum(dic_a.values()) / len(dic_a))
print(dic_a.keys())
print(dic_a.values())