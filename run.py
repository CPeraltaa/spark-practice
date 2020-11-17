from pyspark import SparkContext
import plotly.offline as py
import plotly.graph_objs as go
from datetime import datetime

sc = SparkContext("local", "Simple App")
text_file = sc.textFile("/home/carlospecam/VENTAS.csv")
#######################Reporte 1#########################
reporte1 = text_file.map(lambda line: line.split(",")) \
            .filter(lambda word : (word[2] == 'Cereal') or (word[2] == 'Fruit') or (word[2] =='Meat') or (word[2] =='Snacks') or (word[2] =='Vegetables'))\
             .map(lambda word: (word[2], float(word[11]))) \
             .reduceByKey(lambda a, b: a + b)

output = reporte1.collect()
templist = []
templist2 = []

for val, val2 in output:
    templist.append(val)
    templist2.append(val2)
    print (val, val2)

trace1 = go.Bar(
    x = templist,
    y = templist2,
)

layout1 = go.Layout(
    title=go.layout.Title(
        text='Reporte 1',
        xref='paper',
        x=0
    )
)
data1 = [trace1]
fig1 = go.Figure(data=data1,layout=layout1)
py.plot(fig1, filename='Reporte1.html')
#######################Reporte 2#########################

templist2.clear()
templist.clear()

reporte2 =  text_file.map(lambda line: line.split(",")) \
            .filter(lambda word : (word[0] != 'Region'))\
             .map(lambda word: (word[0], float(word[11]))) \
             .reduceByKey(lambda a, b: a + b)

output2 = reporte2.collect()

for val, val2 in output2:
    templist.append(val)
    templist2.append(val2)
    print (val, val2)

trace2 = go.Pie(
    labels = templist,
    values = templist2,
)

layout2 = go.Layout(
    title=go.layout.Title(
        text='Reporte 2',
        xref='paper',
        x=0
    )
)
data2 = [trace2]
fig2 = go.Figure(data=data2,layout=layout2)
py.plot(fig2, filename='Reporte2.html')
#######################Reporte 3#########################
templist2.clear()
templist.clear()

reporte3 =  text_file.map(lambda line: line.split(",")) \
            .filter(lambda word : (word[0] == 'Central America and the Caribbean') and (word[2] == 'Clothes'))\
             .map(lambda word: (word[1], float(word[11]))) \
             .reduceByKey(lambda a, b: a + b)

output3 = reporte3.collect()

for val, val2 in output3:
    templist.append(val)
    templist2.append(val2)
    print (val, val2)

trace3 = go.Pie(
    labels = templist,
    values = templist2,
)

layout3 = go.Layout(
    title=go.layout.Title(
        text='Reporte 7',
        xref='paper',
        x=0
    )
)
data3 = [trace3]
fig3 = go.Figure(data=data3,layout=layout3)
py.plot(fig3, filename='Reporte3.html')

#######################Reporte 4#########################
templist2.clear()
templist.clear()

reporte4 =  text_file.map(lambda line: line.split(",")) \
            .filter(lambda word : (word[3] != 'Sales Channel'))\
             .map(lambda word: (word[3], int(word[8]))) \
             .reduceByKey(lambda a, b: a + b)

output4 = reporte4.collect()

for val, val2 in output4:
    templist.append(val)
    templist2.append(val2)
    print (val, val2)

trace4 = go.Bar(
    x = templist,
    y = templist2,
)

layout4 = go.Layout(
    title=go.layout.Title(
        text='Reporte 4',
        xref='paper',
        x=0
    )
)
data4 = [trace4]
fig4 = go.Figure(data=data4,layout=layout4)
py.plot(fig4, filename='Reporte4.html')
#######################Reporte 5#########################
templist2.clear()
templist.clear()

reporte5 =  text_file.map(lambda line: line.split(",")) \
            .filter(lambda word : (word[1] == 'Guatemala'))\
             .map(lambda word: (datetime.strptime(word[5], '%m/%d/%Y').year, int(word[8]))) \
             .reduceByKey(lambda a, b: a + b)

output5 = reporte5.collect()

for val, val2 in output5:
    templist.append(val)
    templist2.append(val2)
    print (val, val2)

trace5 = go.Bar(
    x = templist,
    y = templist2,
)

layout5 = go.Layout(
    title=go.layout.Title(
        text='Reporte 5',
        xref='paper',
        x=0
    )
)
data5 = [trace5]
fig5 = go.Figure(data=data5,layout=layout5)
py.plot(fig5, filename='Reporte5.html')

#######################Reporte 6#########################
templist2.clear()
templist.clear()

reporte6 =  text_file.map(lambda line: line.split(",")) \
            .filter(lambda word : (word[4] == 'M'))\
             .map(lambda word: (datetime.strptime(word[5], '%m/%d/%Y').year, 1)) \
             .reduceByKey(lambda a, b: a + b)

output6 = reporte6.collect()

for val, val2 in output6:
    templist.append(val)
    templist2.append(val2)
    print (val, val2)

trace6 = go.Bar(
    x = templist,
    y = templist2,
)

layout6 = go.Layout(
    title=go.layout.Title(
        text='Reporte 6',
        xref='paper',
        x=0
    )
)
data6 = [trace6]
fig6 = go.Figure(data=data6,layout=layout6)
py.plot(fig6, filename='Reporte6.html')
#######################Reporte 7#########################
templist.clear()

revenue =  text_file.map(lambda line: line.split(",")) \
            .filter(lambda word : (word[5] != 'Order Date'))\
            .filter(lambda word : (datetime.strptime(word[5], '%m/%d/%Y').year == 2010))\
            .map(lambda word: (datetime.strptime(word[5], '%m/%d/%Y').year, float(word[11])))\
            .reduceByKey(lambda a, b: a + b)

out1 = revenue.collect()

print(out1)

cost =  text_file.map(lambda line: line.split(",")) \
            .filter(lambda word : (word[5] != 'Order Date'))\
            .filter(lambda word : (datetime.strptime(word[5], '%m/%d/%Y').year == 2010))\
            .map(lambda word: (datetime.strptime(word[5], '%m/%d/%Y').year, float(word[12])))\
            .reduceByKey(lambda a, b: a + b)

out2 = cost.collect()

print(out2)

profit =  text_file.map(lambda line: line.split(",")) \
            .filter(lambda word : (word[5] != 'Order Date'))\
            .filter(lambda word : (datetime.strptime(word[5], '%m/%d/%Y').year == 2010))\
            .map(lambda word: (datetime.strptime(word[5], '%m/%d/%Y').year, float(word[13])))\
            .reduceByKey(lambda a, b: a + b)

out3 = profit.collect()

templist.append(out1[0][1])
templist.append(out2[0][1])
templist.append(out3[0][1])

trace7 = go.Pie(
    labels = ["Total Revenue", "Total Cost", "Total Profit"],
    values = templist,
)
layout7 = go.Layout(
    title=go.layout.Title(
        text='Reporte 7',
        xref='paper',
        x=0
    )
)
data7 = [trace7]
fig7 = go.Figure(data=data7,layout=layout7)
py.plot(fig7, filename='Reporte7.html')
