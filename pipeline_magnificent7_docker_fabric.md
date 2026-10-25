# Informe de Arquitectura y Desarrollo: Pipeline de Datos de los Magnificent Seven en Microsoft Fabric

Este documento detalla de manera integral el ciclo de vida del proyecto de ingeniería de datos desarrollado para la ingesta, procesamiento, enriquecimiento y analítica avanzada de las acciones de los **Magnificent Seven** utilizando un enfoque moderno por capas (Bronze, Silver y Gold) en Microsoft Fabric, combinando datos históricos y de streaming en tiempo real.

---

# Fase 1: Ingesta de datos

## 1. Ingesta Inicial: Contenedor Docker y Almacenamiento Local (Capa Bronze)
El proyecto comenzó con la extracción automatizada de datos históricos del mercado bursátil utilizando la API pública de **yfinance**. 
* **Entorno de Ejecución:** El proceso se configuró dentro de un contenedor de Docker ejecutándose sobre un disco SSD alojado en una máquina virtual con Ubuntu.
* **Persistencia Raw:** Los datos extraídos fueron almacenados localmente en formato `.parquet` dentro de la estructura de la Capa Bronze (`/home/.../data/bronze/`).

![img](img_pipeline/1.%20creacion%20del%20docker%20en%20ubutno%20con%20la%20carpeta%20de%20almacenamienot%20stock_pipeline%20y%20requirements.png)

![img](img_pipeline/2.%20Descaraga%20exitosa%20luego%20del%20estrategia%20de%20cambio%20tiempos%20por%20request%20y%20aliatoriedad%20de%20tiempo%20en%20cada%20una%20de%20ellas%20.png)

![img](img_pipeline/3.%20Verifcamos%20en%20local%20la%20descarga%20de%20los%20archivos.png)
---

# Fase 2: Arquitectura Fabric 

## 1. Reto de Conectividad y Migración a Microsoft Fabric (OneLake)
Debido a las restricciones de conectividad y acceso directo entre los archivos locales de la máquina virtual/SSD hacia la nube de Microsoft Fabric, se evaluaron diversas alternativas de volcado:
* **Intento inicial:** Volcar los archivos `.parquet` directamente hacia la máquina host.
* **Solución exitosa:** Se habilitó una descarga por red a través de un puerto temporal y web temporal para extraer los archivos locales, permitiendo subirlos de forma manual pero efectiva a la sección de archivos (`Files`) del Lakehouse en Fabric, denominado **`stock_bronze_lakehouse`**.

![img](img_pipeline/4.%20Creacion%20de%20un%20servido%20temporal%20hhttp%20para%20obtener%20los%20ficheros%20.parquer.png)


![img](img_pipeline/5.%20Ficheros%20en%20la%20capa%20bronze.png)

![img](img_pipeline/6.%20Ficheros%20en%20la%20capa%20bronze%20abiertos.png)
---

## 2. Procesamiento y Limpieza (Capa Silver: `stock_silver_enriched`)
Una vez que los datos crudos (*raw*) estuvieron disponibles en el Lakehouse de Bronze en Microsoft Fabric, se creó un Notebook de PySpark para estructurar la **Capa Silver**.
* **Acciones realizadas sobre los datos raw:**
  * Lectura de los archivos Parquet originales.
  * Estandarización de nombres de columnas clave (`ticker`, `date`, `open_price`, `high_price`, `low_price`, `closing_price`, `volume`).
  * Creación de columnas normalizadas como `CleanSymbol` (en mayúsculas) y `CleanDate` (formato fecha estándar) para asegurar la integridad referencial y facilitar los cruces posteriores.
  * Consolidación y guardado como una tabla Delta oficial denominada **`stock_silver_enriched`**.

![img](img_pipeline/7.%20Estruturamos%20la%20capa%20silver%20con%20lso%20datos%20historicos%20unicamente%20.png)
---

## 3. Analítica Avanzada y Negocio (Capa Gold: `stock_gold_analytics`)
Partiendo de la tabla limpia de la Capa Silver (`stock_silver_enriched`), se desarrolló un segundo Notebook en PySpark para materializar la **Capa Gold**, orientada por completo al análisis financiero y de negocio.
* **Transformaciones y cálculos aplicados:**
  * **Funciones de Ventana (*Window Functions*):** Particionamiento lógico por cada acción (`ticker`) y ordenamiento cronológico por fecha (`date`).
  * **Retorno Diario Porcentual (`Daily_Return_Pct`):** Cálculo del rendimiento intradía mediante la fórmula:
    $$\frac{\text{closing\_price} - \text{open\_price}}{\text{open\_price}} \times 100$$
  * **Brecha de Precios / Volatilidad (`Price_Spread`):** Medición de la amplitud diaria restando el precio mínimo al máximo (`high_price - low_price`).
  * **Media Móvil Simple (`SMA_5`):** Cálculo del promedio móvil simple del precio de cierre en una ventana deslizante de 5 sesiones bursátiles.
  * **Normalización y Persistencia:** Redondeo de métricas a dos decimales y guardado final como tabla Delta optimizada bajo el nombre **`stock_gold_analytics`**, lista para la capa de visualización en Power BI.

![img](img_pipeline/8.%20estructuramos%20la%20capa%20gold.png)

![img](img_pipeline/9.%20tabla%20de%20explotacion%20capa%20gold%20.png)

---

# Fase 3: Modelado Semántico, Explotación y Distribución en Microsoft Fabric

Este documento detalla la segunda fase del flujo de ingeniería y analítica de datos implementado en Microsoft Fabric. Tomando como base la capa analítica consolidada (Capa Gold), esta fase abarca la estructuración relacional del modelo de datos, la construcción de visualizaciones interactivas en Power BI y la posterior gobernanza y empaquetado de la solución mediante una Aplicación corporativa.

---

## 1. Creación del Modelo Semántico (Semantic Model)

Una vez materializada la tabla analítica en la Capa Gold bajo el nombre `stock_gold_analytics`, se procedió a estructurar el modelo de datos en Microsoft Fabric para dotarlo de contexto de negocio y capacidades relacionales óptimas.

* **Estructura en Estrella (Star Schema):** Se integraron dos entidades principales dentro del entorno de modelado de Fabric:
  1. **Tabla de Hechos (`stock_gold_analytics`):** Contiene los registros transaccionales diarios, precios normalizados y métricas financieras avanzadas calculadas (`Daily_Return_Pct`, `Price_Spread` y la media móvil `SMA_5`).
  2. **Tabla de Dimensiones (`us_tech_activos`):** Aporta los atributos descriptivos de cada activo financiero, permitiendo enriquecer el análisis con el sector tecnológico al que pertenecen y la moneda de transacción (`currency`).
* **Relación y Cardinalidad:** Se estableció una relación relacional activa de **Uno a Varios ($1$ a $*$)** vinculando el campo clave **`ticker`** desde la dimensión de activos hacia la tabla de hechos. Esto asegura una correcta propagación de filtros unidireccionales (`Single`) hacia las métricas financieras sin duplicidad de datos.

![img](img_pipeline/10.%20rwlacion%20ente%20tablas%20del%20modelo%20semantico.png)



![img](img_pipeline/12.linage%20trazabilidad%20de%20los%20datos.png)

---

## 2. Explotación y Visualización de Datos (Power BI Report)

Con el modelo semántico `Magnificent_Seven_Analytics` debidamente configurado, se desarrolló un reporte interactivo en el lienzo web de Power BI con el fin de traducir las métricas técnicas en información visual para la toma de decisiones.

* **Evolución Temporal de Precios:** Gráficos de líneas que trazan el comportamiento de los precios de cierre (`closing_price`) y las medias móviles de corto plazo (`SMA_5`) a lo largo del tiempo para cada componente de los *Magnificent Seven*.
* **Análisis de Volatilidad y Rendimiento:** Tarjetas y gráficos comparativos enfocados en el rendimiento porcentual diario promedio (`Daily_Return_Pct`) y la amplitud de las fluctuaciones de precios mediante el indicador `Price_Spread`.
* **Distribución de Volumen Accionario:** Incorporación de un diagrama de tipo mapa de árbol (*TreeMap*) para dimensionar y comparar de manera visual el volumen total de operaciones (*volume*) agrupado por cada símbolo bursátil.
* **Segmentación Dinámica:** Inclusión de filtros contextuales por fecha, ticker, sector y moneda.


![img](img_pipeline/11.%20reporte%20.png)
---

## 3. Gobernanza, Empaquetado y Distribución (Fabric App)

Para finalizar el ciclo de vida del dato y garantizar un acceso seguro y controlado para los usuarios finales, se empaquetó la solución analítica en una estructura corporativa:

* **Creación de la Aplicación de Fabric:** A partir del espacio de trabajo (*Workspace*), se consolidó el reporte analítico en una **Fabric App** bajo el nombre de *Report Magnificent Seven*.
* **Gestión de Audiencias y Permisos:** Se configuró el acceso para la organización (*Entire organization*), asegurando que cualquier miembro autorizado con el enlace corporativo pueda interactuar con el cuadro de mando.
* **Separación de Entornos:** La publicación de la aplicación aísla a los consumidores finales de la lógica de desarrollo subyacente (notebooks de transformación y tablas en bruto de las capas Bronze/Silver), ofreciendo una interfaz limpia, profesional y orientada estrictamente al consumo analítico.

![img](img_pipeline/13.%20Publicacion%20del%20report%20app.png)
---
## . Incidencias Técnicas, Retos y Soluciones

Durante el desarrollo del proyecto se presentaron diversos retos técnicos que fueron superados estratégicamente:
1. **Problemas de Contexto en Spark (`Bloqueos y Peticiones Denegadas`):**
  * *Causa:* Durante las consultas masivas a la API de yfinance, el sistema comenzó a denegar las solicitudes (requests denied) debido a políticas de limitación de tasa (rate limiting) y detección de scraping automatizado.
  * *Solución:* Se modificó el script de extracción en Python para ampliar el tiempo de espera entre consulta y consulta (sleep intervals), incorporando además un factor de aleatoriedad temporal entre las peticiones. Esto permitió simular un comportamiento de navegación humano y natural, evitando con éxito los bloqueos del servidor y asegurando una descarga limpia de los datos.
2. **Problemas de Contexto en Spark (`No default context found`):**
   * *Causa:* Los Notebooks se ejecutaban sin un Lakehouse adjunto por defecto.
   * *Solución:* Se asoció explícitamente el Lakehouse correspondiente desde el panel lateral de cada Notebook, resolviendo el direccionamiento del espacio de nombres en OneLake.
3. **Errores de Autenticación con el Conector Kusto (`ClientAuthenticationException`):**
   * *Causa:* Conflictos de credenciales al intentar conectar directamente Spark con el Eventhouse en tiempo real.
   * *Solución:* Se optó por una arquitectura híbrida simplificada, exportando los datos recientes del Eventhouse en formato CSV, subiéndolos al Lakehouse e integrándolos mediante operaciones nativas en Spark.
4. **Intentos de Ingesta en Streaming en Vivo:**
   * *Contexto:* Se configuró un Eventhouse y consultas KQL utilizando sintaxis estricta para campos con caracteres especiales (ej. `todatetime(['time'])`) para capturar eventos recientes. Sin embargo, priorizando la estabilidad y velocidad de desarrollo para la culminación analítica, se decidió enfocar el núcleo principal en los datos históricos enriquecidos, dejando las bases preparadas para futuras activaciones del pipeline en tiempo real.

[report app publicado](https://l1nk.dev/m6splef)