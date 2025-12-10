import pandas as pd

#Listas de países a separar

europa=[("Albania","al","alb"),("Hungary","hu","hun"),("Austria","at","aut"),("Belgium","be","bel"),("Bosnia and Herzegovina","ba","bih"),("Bulgaria","bg","bgr"),("Croatia","hr","hrv"),("Czechia","cz","cze"),("France","fr","fra"),("Germany","de","deu"),("Italy","it","ita"),("North Macedonia","mk","mkd"),("Poland","pl","pol"),("Romania","ro","rou"),("Slovakia","sk","svk"),("Slovenia","si","svn"),("Spain","es","esp"),("Switzerland","ch","che"),("Ukraine","ua","ukr"),("United Kingdom of Great Britain and Northen Ireland","gb","gbr"),("Azerbaijan","az","aze"),("Belarus","by","blr"),("Estonia","ee","est"),("Finland","fi","fin"),("Latvia","lv","lva"),("Lithuania","lt","ltu"),("Norway","no","nor"),("Republic of Moldova","md","mda"),("Russian Federation","ru","rus"),("Sweeden","se","swe"),("Netherlands (Kingdom of the)","nl","nld"),("Montenegro","me","mne"),("Serbia","rs","srb"),("Canada","ca","can"),("United States of America","us","usa"),("Denmark","dk","dnk"),("Holy See","va","vat"),("Portugal","pt","prt")]
america_l=[("Argentina","ar","arg"),("Bolivia (Plurinational state of)","bo","bol"),("Chile","cl","chl"),("Colombia","co","col"),("Ecuador","ec","ecu"),("Peru","pe","per"),("Brazil","br","bra"),("Costa Rica","cr","cri"),("Panama","pa","pan")]
asia=[("India","in","ind"),("Japan","jp","jpn"),("Iran (Islamic Republic of)","ir","irn"),("China","cn","chn"),("Kazakhstan","kz","kaz"),("Kyrgystan","kg","kgz"),("Uzbekistan","uz","uzb"),("Turkmenistan","tm","tkm"),("Mongolia","mn","mng"),("Tajikistan","tj","tjk")]
africa=[("Benin","bj","ben"),("Burkina Faso","bf","bfa"),("Niger","ne","ner"),("Togo","tg","tgo"),("Cameroon","cm","cmr"),("Central African Republic","cf","caf"),("Congo","cg","cog"),("Côte d'Ivoire","ci","civ"),("Guinea","gn","gin"),("Gambia","gm","gmb"),("Senegal","sn","sen"),("Lesotho","ls","lso"),("South Africa","za","zaf"),("Zambia","zm","zmb"),("Zimbabue","zw","zwe")]
paises_em_grupo=europa+america_l+asia+africa
euro_codes=[]
ame_l_codes=[]
asia_codes=[]
africa_codes=[]
for i in europa:
    euro_codes+=[i[1]]
for i in america_l:
    ame_l_codes+=[i[1]]
for i in asia:
    asia_codes+=[i[1]]
for i in africa:
    africa_codes+=[i[1]]

#Descrições para cada critério

crit_desc={1:"to represent a masterpiece of human creative genius",
           2:"to exhibit an important interchange of human values, over a span of time or within a cultural area of the world, on developments in architecture or technology, monumental arts, town-planning or landscape design",
           3:"to bear a unique or at least exceptional testimony to a cultural tradition or to a civilization which is living or which has disappeared",
           4:"to be an outstanding example of a type of building, architectural or technological ensemble or landscape which illustrates (a) significant stage(s) in human history",
           5:"to be an outstanding example of a traditional human settlement, land-use, or sea-use which is representative of a culture (or cultures), or human interaction with the environment especially when it has become vulnerable under the impact of irreversible change",
           6:"to be directly or tangibly associated with events or living traditions, with ideas, or with beliefs, with artistic and literary works of outstanding universal significance. (The Committee considers that this criterion should preferably be used in conjunction with other criteria)",
           7:"to contain superlative natural phenomena or areas of exceptional natural beauty and aesthetic importance",
           8:"to be outstanding examples representing major stages of earth's history, including the record of life, significant on-going geological processes in the development of landforms, or significant geomorphic or physiographic features",
           9:"to be outstanding examples representing significant on-going ecological and biological processes in the evolution and development of terrestrial, fresh water, coastal and marine ecosystems and communities of plants and animals",
           10:"to contain the most important and significant natural habitats for in-situ conservation of biological diversity, including those containing threatened species of outstanding universal value from the point of view of science or conservation"}

#Funçoes para adicionar cada alteracao ao dataset
#paises
def add_paises(bd,ind):
    isoc=bd.loc[ind]["iso_code"]
    isoc=isoc.split(",")
    for i in paises_em_grupo:
        if i[1] in isoc:
            bd.loc[len(bd)]=bd.loc[ind]
            bd.loc[len(bd)-1,"states_name_en"]=i[0]
            bd.loc[len(bd)-1,"iso_code"]=i[1]
            bd.loc[len(bd)-1,"udnp_code"]=i[2]
            if i[1] in euro_codes:
                bd.loc[len(bd)-1,"region_en"]="Europe and North America"
            elif i[1] in ame_l_codes:
                bd.loc[len(bd)-1,"region_en"]="Latin America and the Caribbean"
            elif i[1] in asia_codes:
                bd.loc[len(bd)-1,"region_en"]="Asia and the Pacific"
            elif i[1] in africa_codes:
                bd.loc[len(bd)-1,"region_en"]="Africa"

#datas secundarias

def add_datas(bd,i):
    datas=bd.loc[i]["secondary_dates"]
    datas=datas.split(",")
    for d in datas:
        bd.loc[len(bd)]=bd.loc[i]
        bd.loc[len(bd)-1,"secondary_dates"]=d

#periodos de perigo

def add_danger(bd, i):
    datas=bd.loc[i]["danger_list"]
    datas=datas.split("Y")
    datas2=[]
    for d in datas:
        datas2+=d.split("P")
    datas=datas2
    n_pe=0
    for d in datas:
        if len(d)>1:
            bd.loc[len(bd)]=bd.loc[i]
            try:
                bd.loc[len(bd)-1,"perigo_inicio"]=int(d)
            except:
                d=d.split("-")
                bd.loc[len(bd)-1,"perigo_inicio"]=int(d[0])
                bd.loc[len(bd)-1,"perigo_fim"]=int(d[1])
            finally:
                n_pe+=1
    if n_pe==1:
        bd.loc[len(bd)-1,"id_perigo"]=1
    elif n_pe==2:
        min=bd.loc[len(bd)-1]["perigo_inicio"]
        min_ind=1
        max_ind=2
        if bd.loc[len(bd)-2]["perigo_inicio"]<min:
            min_ind=2
            max_ind=1
        bd.loc[len(bd)-min_ind,"id_perigo"]=1
        bd.loc[len(bd)-max_ind,"id_perigo"]=2
            

#criterios

def add_criteria(bd,i):
    for ind in range(1,11):
        if ind<=6:
            cat=("C","Cultural")
            crit="C"+str(ind)
        else:
            cat=("N","Natural")
            crit="N"+str(ind)
        if bd.loc[i][crit]==1:
            bd.loc[len(bd)]=bd.loc[i]
            bd.loc[len(bd)-1,"category_crit_short"]=cat[0]
            bd.loc[len(bd)-1,"criterio"]=crit
            bd.loc[len(bd)-1,"criterio_desc"]=crit_desc[ind]

#criação do dataset final

def create_dataset(path1, path2):
    #ler dataset

    bd=pd.read_excel(path1)

    #acrescentar jerusalem como país

    bd.loc[625,"iso_code"]="jr"
    bd.loc[625,"udnp_code"]="jrm"

    #acrescentar cada uma das alteracoes
    #paises
    n_linhas=len(bd)
    bd_c=bd.copy()  
    for i in range(n_linhas):
        if len(bd.loc[i]["iso_code"])>2:
            add_paises(bd,i)
            bd_c=bd_c[bd_c["id_no"]!=bd.loc[i]["id_no"]]
    bd=pd.concat([bd_c,bd.loc[n_linhas:]],ignore_index=True)

    #datas secundarias
    n_linhas=len(bd)
    bd_c=bd.copy()
    lugares_com_datas=pd.isna(bd["secondary_dates"])
    for i in range(n_linhas):
        if not lugares_com_datas[i] and len(bd.loc[i]["secondary_dates"])>4:
            add_datas(bd,i)
            bd_c=bd_c[bd_c["id_no"]!=bd.loc[i]["id_no"]]
    bd=pd.concat([bd_c,bd.loc[n_linhas:]],ignore_index=True)
    bd["secondary_dates"].astype("float64")

    #periodos de perigo
    n_linhas=len(bd)
    bd_c=bd.copy()
    lugares_com_perigos=pd.isna(bd["danger_list"])
    for i in range(n_linhas):
        if not lugares_com_perigos[i]:
            add_danger(bd,i)
            bd_c=bd_c[bd_c["id_no"]!=bd.loc[i]["id_no"]]
    bd=pd.concat([bd_c,bd.loc[n_linhas:]],ignore_index=True)

    #criterios
    n_linhas=len(bd)
    for i in range(n_linhas):
        add_criteria(bd,i)
    bd=bd.loc[n_linhas:]

    #reiniciar os indexes do dataset
    bd=bd.reset_index()
    cols=bd.columns
    bd=bd[cols[1:]]

    for i in range(len(bd)):
        if bd.loc[i]["criterio_desc"][0]=='"':
                bd.loc[i,"criterio_desc"]=bd.loc[i]["criterio_desc"][1:-1]

    #criar o novo dataset
    bd.to_csv(path2,sep=",")