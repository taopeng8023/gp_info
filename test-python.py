# # k = [1,2,3]
# # k[0] = 'user name'
# # print(k)
# #
# # s = {1,3,4}
# # print(s)
# # s.add((2,5,6))
# # for x in s:
# #     if isinstance(x,tuple):
# #         for y in x:
# #             if y == 6:
# #                 print(y)
# #
# # print(abs(-100))
#
# # def test_Add(a,b):
# #     return a + b,a*b
# #
# # mm = test_Add(1,2)
# # print(mm)
#
#
# import math
#
#
# def quadratic(a,b,c):
#     r = b ** 2 - 4 * a * c
#     if r < 0:
#         print("方程无实数解")
#         return (-1,-1)
#     elif r == 0:
#         x = -b / (2*a)
#         return x,x
#     else:
#         sqrt_disc = math.sqrt(r)
#         x1 = (-b + sqrt_disc) / (2 * a)
#         x2 = (-b - sqrt_disc) / (2 * a)
#         return x1,x2
#
# print(quadratic(2,4,1))
from functools import reduce


# L1 = ['Hello', 'World', 18, 'Apple', None]
# L2 = [X.lower() for X in L1  if isinstance(X,str) == True]
# # 测试:
# print(L2)
# if L2 == ['hello', 'world', 'apple']:
#     print('测试通过!')
# else:
#     print('测试失败!')




# 期待输出:
# [1]
# [1, 1]
# [1, 2, 1]
# [1, 3, 3, 1]
# [1, 4, 6, 4, 1]
# [1, 5, 10, 10, 5, 1]
# [1, 6, 15, 20, 15, 6, 1]
# [1, 7, 21, 35, 35, 21, 7, 1]
# [1, 8, 28, 56, 70, 56, 28, 8, 1]
# [1, 9, 36, 84, 126, 126, 84, 36, 9, 1]
# def triangles(n):
#     if n == 1:
#         yield [1]
#     elif n == 2:
#         yield [1,1]
#     else:
#         yield [1]
#         yield [1, 1]
#         s = [1,1]
#         x = 3
#         while x <= n:
#             s =[1] + [s[i] + s[i+1]  for i in range(len(s)-1)] +[1]
#             yield s
#             x = x+1
#
#
#
# gg = triangles(4)
# results =[]
# for t in gg:
#     results.append(t)
# print(results)



# def normalize(name):
#     return name[0].upper() + name[1:].lower()
#
# # 测试:
# L1 = ['adam', 'LISA', 'barT']
# L2 = list(map(normalize, L1))
# print(L2)


# def mu(x,y):
#     return x * y
# def prod(L):
#     return reduce(mu,L)
#
# print('3 * 5 * 7 * 9 =', prod([3, 5, 7, 9]))
# if prod([3, 5, 7, 9]) == 945:
#     print('测试成功!')
# else:
#     print('测试失败!')


def str2float(s):
    a=s.find('.')
    s=s[:a]+s[a+1:]
    print(s)
    def my_num(v):
        d={'0':0, '1':1, '2':2, '3':3, '4':4, '5':5, '6':6, '7':7, '8':8, '9':9}
        return d[v]
    def f(x,y):
        return 10*x+y
    return reduce(f,map(my_num,s))/10**(len(s)-a)

if abs(str2float('123.456') - 123.456) < 0.00001:
    print('测试成功!')
else:
    print('测试失败!')