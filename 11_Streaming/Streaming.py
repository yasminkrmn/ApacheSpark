import findspark
findspark.init()

from pyspark.streaming import *
from pyspark import SparkContext
import warnings
warnings.filterwarnings("ignore")

spark = SparkContext(appName="StreamingQuickExample", master="local[4]")

streaming_context = StreamingContext(spark, batchDuration=10)

lines = streaming_context.textFileStream(r"C:\Users\gayan\Documents\apache_spark\11_Streaming")

lines.pprint(30)

streaming_context.start()
# -------------------------------------------
# Time: 2024-03-19 15:57:10
# -------------------------------------------
streaming_context.awaitTermination()

#                                                                                -------------------------------------------
# Time: 2024-03-19 15:59:00
# -------------------------------------------
# Ömer Seyfettin -  Forsa
# Akdeniz’in, kahramanlık yuvası sonsuz ufuklarına bakan küçük tepe, minimini bir çiçek ormanı gibiydi. İnce uzun dallı badem ağaçlarının alaca gölgeleri sahile inen keçiyoluna düşüyor, ilkbaharın tatlı rüzgârıyla sarhoş olan martılar, çılgın bağrışlarıyla havayı çınlatıyordu. Badem bahçesinin yanı geniş bir bağdı. Beyaz taşlardan yapılmış kısa bir duvarın ötesindeki harabe vadiye kadar iniyordu. Bağın ortasındaki yıkık kulübenin kapısız girişinden bir ihtiyar çıktı. Saçı sakalı bembeyazdı. Kamburunu düzeltmek istiyormuş gibi gerindi. Elleri, ayakları titriyordu. Gök kadar boş, gök kadar sakin duran denize baktı, baktı.
# – Hayırdır inşallah! dedi.
# Duvarın dibindeki taş yığınlarına çöktü. Başını ellerinin arasına aldı. Sırtında yırtık bir çuval vardı. Çıplak ayakları topraktan yoğrulmuş gibiydi. Zayıf kolları kirli tunç rengindeydi. Yine başını kaldırdı. Gökle denizin birleştiği dumandan çizgiye dikkatle baktı, Ama görünürde bir şey yoktu.
# Bu, her gece uykusunda onu kurtarmak için birçok geminin pupa yelken geldiğini gören zavallı eski bir Türk forsasıydı. Tutsak olalı kırk yılı geçmişti. Otuz yaşında, dinç, levent, güçlü bir kahramanken Malta korsanlarının eline düşmüştü. Yirmi yıl onların kadırgalarında kürek çekti. Yirmi yıl iki zincirle iki ayağından rutubetli bir geminin dibine bağlanmış yaşadı. Yirmi yılın yazları, kışları, rüzgârları, fırtınaları, güneşleri onun granit vücudunu eritemedi. Zincirleri küflendi, çürüdü, kırıldı. Yirmi yıl içinde birkaç kez halkalarını, çivilerini değiştirdiler. Ama onun çelikten daha sert kaslı bacaklarına bir şey olmadı. Yalnız aptes alamadığı için. üzülüyordu. Hep güneşin doğduğu yanı sol ilerisine alır, gözlerini kıbleye çevirir, beş vakit namazı gizli işaretle yerine getirirdi.
#
# Elli yaşına gelince, korsanlar onu, “Artık iyi kürek çekemez!” diye bir adada satmışlardı. Efendisi bir çiftçiydi. On yıl kuru ekmekle onun yanında çalıştı. Tanrıya şükrediyordu. Çünkü artık bacaklarından mıhlı değildi. Aptes alabiliyor, tam kıblenin karşısına geçiyor, unutmadığı âyetlerle namaz kılıyor, dua edebiliyordu. Bütün umudu, doğduğu yere, Edremit’e kavuşmaktı. Otuz yıl içinde bir an bile umudunu kesmedi. “Öldükten sonra dirileceğime nasıl inanıyorsam, öyle inanıyorum, elli yıl tutsaklıktan sonra da ülkeme kavuşacağıma öyle inanıyorum!” derdi. En ünlü, en tanınmış Türk gemicilerdendi. Daha yirmi yaşındayken, Tarık Boğazı’nı geçmiş, poyraza doğru haftalarca, aylarca, kenar kıyı görmeden gitmiş, rast geldiği ıssız adalardan vergiler almış, irili ufaklı donanmaları tek başına hafif gemisiyle yenmişti. O zamanlar Türkeli’nde nâmı dillere destandı. Padişah bile onu, saraya çağırtmıştı. Serüvenlerini dinlemişti. Çünkü o, Hızır Aleyhisselâm’ın gittiği diyarları dolaşmıştı. Öyle denizlere gitmişti ki, üzerinde dağlardan, adalardan büyük buz parçaları yüzüyordu. Oraları tümüyle başka bir dünyaydı. Altı ay gündüz, altı ay gece olurdu! Karısını, işte bu, yılı bir büyük günle bir büyük geceden oluşan başka dünyadan almıştı.
#
# Gemisi altın, gümüş, inci, elmas, tutsak dolu vatana dönerken deniz ortasında evlenmiş, oğlu Turgut, Çanakkale’yi geçerken doğmuştu. Şimdi kırk beş yaşında olmalıydı. Acaba yaşıyor muydu? Hayalini unuttuğu, karlardan beyaz karısı acaba sağ mıydı? Kırk yıldır, yalnız taht yerinin, İstanbul’un minareleri, ufku, hayalinden hiç silinmemişti. “Bir gemim olsa gözümü kapar, Kabataş’ın önüne demir atarım” diye düşünürdü. Altmış yaşını geçtikten sonra efendisi, onu sözde özgür kıldı. Bu özgür kılmak değil, sokağa, perişanlığa atmaktı, Yaşlı tutsak bu bakımsız bağın içindeki yıkık kulübeyi buldu. İçine girdi. Kimse bir şey demedi. Ara sıra kasabaya iniyor, yaşlılığına acıyanların verdiği ekmek paralarını toplayıp dönüyordu. On yıl daha geçti. Artık hiç gücü kalmamıştı. Hem bağ sahibi de artık onu istemiyordu. Nereye gidecekti?
#
# Ama işte, eskiden beri gördüğü rüyaları yine görmeye başlamıştı. Kırk yıllık bir rüya… Türklerin, Türk gemilerinin gelişi… Gözlerini kurumuş elleriyle iyice ovdu. Denizin gökle birleştiği yere baktı. Evet, geleceklerdi, kesindi bu, buna öylesine inanıyordu ki…
# – Kırk yıl görülen bir rüya yalan olamaz! diyordu. Kulübe duvarının dibine uzandı. Yavaş yavaş gözlerini kapadı. İlkbahar bir umut tufanı gibi her yanı parlatıyordu. Martıların, “Geliyorlar, geliyorlar, seni kurtarmaya geliyorlar!” gibi işittiği tatlı seslerini dinleye dinleye daldı. Duvar taşlarının arkasından çıkan kertenkeleler üzerinde geziniyorlar, çuvaldan giysinin içine kaçıyorlardı, gür, beyaz sakalının üstünde oynaşıyorlardı. Yaşlı tutsak rüyasında, ağır bir Türk donanmasının limana girdiğini görüyordu. Kasabaya giden yola birkaç bölük asker çıkarmışlardı. Al bayrağı uzaktan tanıdı. Yatağanlar, kalkanlar güneşin yansımasıyla parlıyordu.
# Bizimkiler! Bizimkiler! diye bağırarak uyandı. Doğruldu. Üstündeki kertenkeleler kaçıştılar. Limana baktı. Gerçekten, kalenin karşısında bir donanma gelmişti. Kadırgaların, yelkenlerin, küreklerin biçimine dikkat etti. Sarardı. Gözlerini açtı. Yüreği hızla çarpmaya başladı. Ellerini göğsüne koydu. Bunlar Türk gemileriydi. Kıyıya yanaşıyorlardı. Gözlerine inanamadı.
# “Acaba rüyada mıyım?” kuşkusuna kapıldı. Uyanıkken rüya görülür müydü? İyice inanabilmek amacıyla elini ısırdı. Yerden sivri bir taş parçası aldı. Alnına vurdu. Evet, işte hissediyordu. Uyanıktı. Gördüğü rüya değildi. O uyurken, donanma burnun arkasından birdenbire çıkıvermiş olacaktı. Sevinçten, şaşkınlıktan dizlerinin bağı çözüldü. Hemen çöktü. Karaya çıkan bölükler, ellerinde al bayraklar, kaleye doğru ilerliyorlardı. Kırk yıllık bir beklemenin son çabasıyla davrandı. Birden kemikleri çatırdadı. Badem ağaçlarının çiçekli gölgeleriyle örtülen yoldan yürüdü. Kıyıya doğru koştu, koştu. Karaya çıkan askerler, aksakallı bir ihtiyarın kendilerine doğru koştuğuna görünce:
# – Dur! diye bağırdılar. İhtiyar durmadı, bağırdı:
# -------------------------------------------
# Time: 2024-03-19 15:59:10
# -------------------------------------------

