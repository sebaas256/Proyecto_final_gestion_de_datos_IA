
# Política de Gobernanza - Proyecto DevOps Metrics

## 1. Propósito
Definir cómo se utilizan, protegen y comparten los datos del proyecto , con el objetivo de crear 
metricas y dashboard seguros para un buen analisis

## 2. Clasificación
- Público: puede compartirse sin restricciones como la Columna avg_availability_pct
- Interno: uso dentro del equipo 
- Confidencial: requiere protección (company_id, total_downtime_min)
- Restringido: acceso limitado, ej: el Parquet original

## 3. Acceso por Rol
- Líder: Tiene acceso total para la toma de decisiones y definición de la política de datos.
- Ingeniero de Datos: Acceso a los datos crudos originales para aplicar scripts de limpieza y minimización.
- Analista de Datos y Calidad: Solo vista protegida (sin identificadores) para crear reportes y validar que no haya fugas de información.
- Docente: Solo lectura de evidencias (Matriz de Riesgos y CSV final) para evaluación y calificación del proyecto.

## 4. Minimización
* Se aplicó una técnica de selección estricta ("lista blanca") en el código para excluir por completo la 
métrica confidencial de inactividad (total_downtime_min) de la vista analítica.

## 5. Retención
Los datos se conservan durante el ciclo IV 2026 y se eliminan al finalizar dicho ciclo el 20 de noviembre de 2026. 

## 6. Ética
No se utilizan variables sin finalidad justificada ni se presentan conclusiones sin contexto. 
