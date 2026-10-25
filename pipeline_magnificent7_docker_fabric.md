# Informe de Arquitectura y Desarrollo: Pipeline de Datos de los Magnificent Seven en Microsoft Fabric

Este documento detalla de manera integral el ciclo de vida del proyecto de ingeniería de datos desarrollado para la ingesta, procesamiento, enriquecimiento y analítica avanzada de las acciones de los **Magnificent Seven** utilizando un enfoque moderno por capas (Bronze, Silver y Gold) en Microsoft Fabric, combinando datos históricos y de streaming en tiempo real.

---

## 1. Ingesta Inicial: Contenedor Docker y Almacenamiento Local (Capa Bronze)
El proyecto comenzó con la extracción automatizada de datos históricos del mercado bursátil utilizando la API pública de **yfinance**. 
* **Entorno de Ejecución:** El proceso se configuró dentro de un contenedor de Docker ejecutándose sobre un disco SSD alojado en una máquina virtual con Ubuntu.
* **Persistencia Raw:** Los datos extraídos fueron almacenados localmente en formato `.parquet` dentro de la estructura de la Capa Bronze (`/home/.../data/bronze/`).

---

## 2. Reto de Conectividad y Migración a Microsoft Fabric (OneLake)
Debido a las restricciones de conectividad y acceso directo entre los archivos locales de la máquina virtual/SSD hacia la nube de Microsoft Fabric, se evaluaron diversas alternativas de volcado:
* **Intento inicial:** Volcar los archivos `.parquet` directamente hacia la máquina host.
* **Solución exitosa:** Se habilitó una descarga por red a través de un puerto temporal y web temporal para extraer los archivos locales, permitiendo subirlos de forma manual pero efectiva a la sección de archivos (`Files`) del Lakehouse en Fabric, denominado **`stock_bronze_lakehouse`**.

---

## 3. Procesamiento y Limpieza (Capa Silver: `stock_silver_enriched`)
Una vez que los datos crudos (*raw*) estuvieron disponibles en el Lakehouse de Bronze en Microsoft Fabric, se creó un Notebook de PySpark para estructurar la **Capa Silver**.
* **Acciones realizadas sobre los datos raw:**
  * Lectura de los archivos Parquet originales.
  * Estandarización de nombres de columnas clave (`ticker`, `date`, `open_price`, `high_price`, `low_price`, `closing_price`, `volume`).
  * Creación de columnas normalizadas como `CleanSymbol` (en mayúsculas) y `CleanDate` (formato fecha estándar) para asegurar la integridad referencial y facilitar los cruces posteriores.
  * Consolidación y guardado como una tabla Delta oficial denominada **`stock_silver_enriched`**.

---

## 4. Analítica Avanzada y Negocio (Capa Gold: `stock_gold_analytics`)
Partiendo de la tabla limpia de la Capa Silver (`stock_silver_enriched`), se desarrolló un segundo Notebook en PySpark para materializar la **Capa Gold**, orientada por completo al análisis financiero y de negocio.
* **Transformaciones y cálculos aplicados:**
  * **Funciones de Ventana (*Window Functions*):** Particionamiento lógico por cada acción (`ticker`) y ordenamiento cronológico por fecha (`date`).
  * **Retorno Diario Porcentual (`Daily_Return_Pct`):** Cálculo del rendimiento intradía mediante la fórmula:
    $$\frac{\text{closing\_price} - \text{open\_price}}{\text{open\_price}} \times 100$$
  * **Brecha de Precios / Volatilidad (`Price_Spread`):** Medición de la amplitud diaria restando el precio mínimo al máximo (`high_price - low_price`).
  * **Media Móvil Simple (`SMA_5`):** Cálculo del promedio móvil simple del precio de cierre en una ventana deslizante de 5 sesiones bursátiles.
  * **Normalización y Persistencia:** Redondeo de métricas a dos decimales y guardado final como tabla Delta optimizada bajo el nombre **`stock_gold_analytics`**, lista para la capa de visualización en Power BI.

---

## 5. Incidencias Técnicas, Retos y Soluciones

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
