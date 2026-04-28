# Claude Portfolio — Sistema Autónomo de Gestión de Cartera

Sistema de rebalanceo semanal de portfolio basado en datos
reales de mercado y análisis con LLMs (Groq + Gemini).
Se ejecuta automáticamente cada lunes en GitHub Actions
y envía un informe completo por email.

---

## ¿Qué hace exactamente?

Cada lunes a las 9:00 UTC el sistema:

1. Descarga los ~1.000 componentes del **Russell 1000**
   desde Wikipedia
2. Obtiene **datos históricos reales** (1 año) de precios
   via `yfinance`
3. Descarga **fundamentales reales** (PE, growth, márgenes,
   ROE, D/E...) en paralelo para todos los tickers
4. Aplica un **pre-filtro cuantitativo** (momentum,
   calidad, valoración) para reducir a ~60 candidatos
5. Usa un **LLM** (Groq/Gemini) para puntuar los
   candidatos en batches
6. Construye **escenarios probabilísticos** bull/base/bear
   a 1/3/6/12 meses para los 30 mejores
7. **Optimiza el portfolio** con `scipy` (SLSQP)
   maximizando el retorno ajustado por riesgo
8. Genera **tesis de inversión** para cada operación
9. Calcula **performance real vs SPY** desde la fecha
   de entrada de cada posición
10. Envía un **informe HTML completo** por email con
    todo lo anterior

> ⚠️ **Aviso:** Esto no es un consejo de inversión.
> Es un sistema personal de gestión de cartera propia.

---

## Pipeline completo
