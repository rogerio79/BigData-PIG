/* COMANDOS HDFS
hdfs dfs -mkdir hadoop hadoop/trabalho hadoop/trabalho/dados
hdfs dfs -put *.csv hadoop/trabalho/dados
pig temperatura.pig
*/

registros = LOAD 'hadoop/trabalho/dados/curitiba-2016.csv' USING PigStorage(';') AS 
(estacao:int, data:chararray, hora:int, precipitacao:int, tempmax:float, tempmin:float, isolacao:float, umidrelativa:float, velventomedia:float);

/* 
   O dataset curitiba-2016.csv traz dois registros de temperatura por dia, a mínima e a máxima. E grava cada um deles em linhas diferentes, gerando	
   duas linhas com o mesmo dia, mas uma com a temperatura máxima e outra com a mínima.
   Quando a temp. máxima é gravada a mínima da linha fica em branco e vice-versa. Por isto a necessidade de fazer algumas alterações nos dados.	
   Também foi necessário transformar o campo data do padrão dd/mm/aaaa gravado para um padrão aceito pelo PIG 
   para utilizar a função de ordenação.
*/

registros_sub = FOREACH registros 
		GENERATE 
		FLATTEN (STRSPLIT(data,'/')) as (dia:chararray, mes:chararray, ano:chararray),  --transforma a data dd/mm/aaaa em campos da tupla:dia mes ano
		(tempmax IS NOT NULL ? tempmax : tempmin) as temp,    --seleciona o valor não nulo, tempmax ou tempmin
		(precipitacao IS NOT NULL ? precipitacao : 0) as chuva;    --se for nulo, preenche com zero
				 

reg_group_date = GROUP registros_sub 
		 BY (dia,mes,ano); --realiza um agrupamento composto

data_temp = FOREACH reg_group_date  --gera o campo q foi agrupado e a média da bag das temperaturas
			GENERATE 
			group as data, 
			AVG (registros_sub.temp) as tempdia,  
			SUM (registros_sub.chuva) as chuva;


transforma_data = FOREACH data_temp  --concatena o valor dos campos da tupla gerando uma data compatível PIG
					GENERATE 
					ToDate(CONCAT(data.ano,'/',data.mes,'/',data.dia),'yyyy/MM/dd'), 
					tempdia,
					chuva;
temperaturas_ordenadas_data = ORDER transforma_data BY $0;


/* *********** MEDIA DA TEMPERATURA POR MÊS e TOTAl DE CHUVA NO MÊS EM mm  *** */
data_temp_mes = FOREACH (GROUP registros_sub BY mes) 
				GENERATE	
				group, 
				AVG(registros_sub.temp) as tempmedia,
				SUM(registros_sub.chuva) as totalchuva;
/* ***************************************** */


STORE temperaturas_ordenadas_data INTO 'hadoop/trabalho/temperaturas' USING PigStorage(',');

STORE data_temp_mes INTO 'hadoop/trabalho/temp_chuva_mes' USING PigStorage(',');








