// dashboard.js - VERSIÓN FINAL COMPLETA Y CORREGIDA

// Configuración global
const API_BASE_URL = 'https://pm-dashb-7.onrender.com';

let lastUpdateTime = null;
let ganttZoomLevel = 'days'; // 'days' | 'weeks' | 'months'

// Paleta de colores para estados
const statusColors = {
  'TO_DO': '#FF0AC4',     // Rojo claro (Alerta)
  'IN_PROGRESS': '#50F8FA', // Azul intermedio (Trabajo activo)
  'BLOCKED': '#0B0D0C',   // Amarillo (Advertencia)
  'COMPLETED': '#27E568', // Verde oscuro (Éxito)
  'CANCELLED': '#0B0D0C'  // Gris (Neutro)
};

// Helpers robustos para el Donut/Pie Chart
const getPieStatus = (d) => {
  const status = String(d.data._id || d.data.status || d.data.name || 'TO_DO').toUpperCase();
  return status.includes('TO_DO') ? 'TO_DO' :
         status.includes('IN_PROGRESS') ? 'IN_PROGRESS' :
         status.includes('COMPLETED') ? 'COMPLETED' :
         status.includes('BLOCKED') ? 'BLOCKED' :
         status.includes('CANCELLED') ? 'CANCELLED' : 'TO_DO';
};

const getPieLabel = (d) => {
  const label = String(d.data._id || d.data.status || d.data.name || 'Indefinido');
  return label
    .replace('_', ' ')
    .replace('TO_DO', 'Por Hacer')
    .replace('IN_PROGRESS', 'En Progreso')
    .replace('COMPLETED', 'Completado')
    .replace('BLOCKED', 'Bloqueado')
    .replace('CANCELLED', 'Cancelado');
};

// Función helper mejorada para obtener color
function getColorForStatus(status) {
  const statusUpper = String(status || '').toUpperCase();
  return statusColors[statusUpper] || 
         statusColors[statusUpper.replace(' ', '_')] || 
         '#cccccc';
}

// Función helper mejorada para etiqueta
function getLabelForStatus(status) {
  const statusStr = String(status || 'Indefinido');
  return statusStr
    .replace('_', ' ')
    .replace('TO_DO', 'Por Hacer')
    .replace('IN_PROGRESS', 'En Progreso')
    .replace('COMPLETED', 'Completado')
    .replace('BLOCKED', 'Bloqueado')
    .replace('CANCELLED', 'Cancelado');
}

// --- FUNCIÓN GLOBAL DE ZOOM ---
function zoomGantt(level) {
  ganttZoomLevel = level;
  const zoomSelect = document.getElementById('gantt-zoom-level');
  if (zoomSelect) zoomSelect.value = level;
  if (typeof renderGanttChart === 'function') renderGanttChart();
  document.querySelectorAll('.gantt-zoom-active').forEach(el => el.classList.remove('gantt-zoom-active'));
  const btn = document.querySelector(`[data-zoom="${level}"]`);
  if (btn) btn.classList.add('gantt-zoom-active');
}
window.zoomGantt = zoomGantt;

/**
 * Función CRÍTICA: Resetea los filtros de Estado y Usuario y refresca el Gantt.
 * EXPUESTA GLOBALMENTE para el onclick="resetFiltersAndRefresh()"
 */
function resetFiltersAndRefresh() {
    const filterElement = document.getElementById('status-filter');
    const userFilterElement = document.getElementById('user-filter');

    if (filterElement) filterElement.value = '';
    if (userFilterElement) userFilterElement.value = ''; 

    renderGanttChart();
}
window.resetFiltersAndRefresh = resetFiltersAndRefresh; 

// Helper para parsear la fecha (utilizado en Scoreboard y Tareas Próximas)
function parseDateFlexible(dateString) {
    if (!dateString) return null;
    let date = new Date(String(dateString).trim());
    // Limpiar la hora para la comparación
    if (!isNaN(date.getTime())) date.setHours(0, 0, 0, 0); 
    return isNaN(date.getTime()) ? null : date;
}

// Helper robusto para obtener el estado de una tarea (uso en Workload, Overdue, Gantt)
const getTaskStatus = (task) => String(task.status || task._id || 'TO_DO').toUpperCase();

// =======================================================
// 1. CARGA Y SOBREESCRITURA DE CSV
// =======================================================
async function handleIngestCsv() {
  const fileInput = document.getElementById('csv-file-input');
  const statusDiv = document.getElementById('ingestion-status');

  if (!statusDiv) return;
  statusDiv.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Subiendo archivo...';
  statusDiv.style.color = '#333';

  if (!fileInput || fileInput.files.length === 0) {
    statusDiv.innerHTML = 'Por favor, selecciona un archivo CSV.';
    statusDiv.style.color = 'red';
    return;
  }

  const file = fileInput.files[0];
  const formData = new FormData();
  formData.append('file', file);

  try {
    const response = await fetch(`${API_BASE_URL}/api/ingest/tasks`, {
      method: 'POST',
      body: formData,
    });

    const result = await response.json();

    if (response.ok && result.status === 'success') {
      statusDiv.innerHTML = `<i class="fas fa-check-circle"></i> ${result.message}`;
      await loadAllData();
      fileInput.value = '';
    } else {
      const errorMessage = result.detail || result.message || `Error del servidor (${response.status} ${response.statusText}).`;
      statusDiv.innerHTML = `<i class="fas fa-times-circle"></i> Fallo en la ingesta: ${errorMessage}`;
      statusDiv.style.color = 'red';
    }
  } catch (error) {
    console.error("Error de red o CORS al subir el archivo:", error);
    statusDiv.innerHTML = `<i class="fas fa-exclamation-circle"></i> Error de conexión con la API. Revise la consola del navegador.`;
    statusDiv.style.color = 'red';
  }
}

// =======================================================
// 2. ESTADO DEL PROYECTO (Donut Chart) - VERSIÓN CORREGIDA DEFINITIVA
// =======================================================
async function renderProjectStatus() {
  try {
    console.log('🎯 renderProjectStatus: Iniciando...');
    
    const response = await fetch(`${API_BASE_URL}/api/project/status`);
    const responseData = await response.json();
    const data = responseData.projects || [];

    const container = d3.select('#project-status-chart');
    container.html('');

    if (!Array.isArray(data) || data.length === 0) {
      container.append('div')
        .attr('class', 'no-data-message')
        .style('padding', '40px')
        .style('text-align', 'center')
        .style('color', '#666')
        .style('font-style', 'italic')
        .text('No hay datos de proyectos disponibles.');
      console.log('⚠️ renderProjectStatus: No hay datos de proyectos');
      return;
    }

    // Consolidar datos (agrupar por estado, ignorando proyecto)
    const statusMap = new Map();
    
    data.forEach(project => {
      if (project?.statuses?.length) {
        project.statuses.forEach(statusItem => {
          if (statusItem?.status && statusItem.count > 0) {
            const status = statusItem.status.toUpperCase();
            const count = Number(statusItem.count);
            
            if (!statusMap.has(status)) {
              statusMap.set(status, {
                status: status,
                count: 0,
                label: status.replace('_', ' ')
              });
            }
            statusMap.get(status).count += count;
          }
        });
      }
    });

    // Convertir a array y ordenar por cantidad
    const pieData = Array.from(statusMap.values())
      .sort((a, b) => b.count - a.count);
    
    const totalTasks = pieData.reduce((sum, d) => sum + d.count, 0);

    console.log('📊 renderProjectStatus: Datos procesados:', {
      items: pieData.length,
      totalTasks: totalTasks,
      datos: pieData
    });

    if (pieData.length === 0) {
      container.append('div')
        .attr('class', 'no-data-message')
        .style('padding', '40px')
        .style('text-align', 'center')
        .style('color', '#666')
        .style('font-style', 'italic')
        .text('No hay datos de estados para mostrar.');
      console.log('⚠️ renderProjectStatus: No hay datos válidos para gráfico');
      return;
    }

    // Configurar dimensiones dinámicas
    const containerElement = document.getElementById('project-status-chart');
    const containerWidth = containerElement?.clientWidth || 400;
    const containerHeight = Math.max(containerElement?.clientHeight || 0, 350);
    
    const width = Math.min(containerWidth, 500);
    const height = Math.min(containerHeight, 350);
    const radius = Math.min(width, height) / 2 - 50;
    const innerRadius = radius * 0.5;

    console.log('📏 renderProjectStatus: Dimensiones:', {
      containerWidth,
      containerHeight,
      width,
      height,
      radius,
      innerRadius
    });

    // Crear SVG
    const svg = container.append('svg')
      .attr('width', width)
      .attr('height', height)
      .style('display', 'block')
      .style('margin', '0 auto')
      .style('overflow', 'visible');

    const g = svg.append('g')
      .attr('transform', `translate(${width / 2}, ${height / 2})`);

    // Definir colores mejorados (más contraste)
    const colorPalette = {
      'TO_DO': '#FF6B6B',      // Rojo coral
      'IN_PROGRESS': '#4ECDC4', // Turquesa
      'COMPLETED': '#06D6A0',   // Verde esmeralda
      'BLOCKED': '#FFD166',     // Amarillo mostaza
      'CANCELLED': '#6C757D'    // Gris
    };

    const color = d3.scaleOrdinal()
      .domain(pieData.map(d => d.status))
      .range(pieData.map(d => colorPalette[d.status] || '#CCCCCC'));

    // Crear gráfica de donut
    const pie = d3.pie()
      .value(d => d.count)
      .sort(null);

    const arc = d3.arc()
      .innerRadius(innerRadius)
      .outerRadius(radius);

    const arcs = g.selectAll('.arc')
      .data(pie(pieData))
      .enter().append('g')
      .attr('class', 'arc')
      .style('cursor', 'pointer');

    // Dibujar segmentos con mejor contraste
    arcs.append('path')
      .attr('d', arc)
      .attr('fill', d => color(d.data.status))
      .style('stroke', '#fff')
      .style('stroke-width', '2px')
      .style('transition', 'opacity 0.3s, transform 0.3s')
      .on('mouseover', function(event, d) {
        d3.select(this)
          .style('opacity', 0.8)
          .style('transform', 'scale(1.05)');

        const percentage = ((d.data.count / totalTasks) * 100).toFixed(1);
        d3.select('#tooltip')
          .style('opacity', 1)
          .html(`
            <div style="font-weight: bold; margin-bottom: 5px; color: ${color(d.data.status)}">
              ${d.data.label}
            </div>
            <div style="font-size: 14px;">
              <strong>${d.data.count}</strong> tareas<br>
              <strong>${percentage}%</strong> del total
            </div>
          `)
          .style('left', (event.pageX + 15) + 'px')
          .style('top', (event.pageY - 15) + 'px');
      })
      .on('mouseout', function() {
        d3.select(this)
          .style('opacity', 1)
          .style('transform', 'scale(1)');
        d3.select('#tooltip').style('opacity', 0);
      })
      .on('click', function(event, d) {
        const status = d.data.status;
        const filterElement = document.getElementById('status-filter');
        const userFilterElement = document.getElementById('user-filter');

        if (filterElement) {
          const newValue = filterElement.value === status ? '' : status;
          filterElement.value = newValue;
          if (userFilterElement) userFilterElement.value = '';
          
          // Feedback visual
          d3.selectAll('.arc path')
            .style('opacity', 0.3);
          
          if (newValue) {
            d3.select(this)
              .style('opacity', 1)
              .style('stroke-width', '3px')
              .style('stroke', '#333');
          } else {
            d3.selectAll('.arc path')
              .style('opacity', 1)
              .style('stroke-width', '2px');
          }
          
          renderGanttChart();
        }
      });

    // Agregar porcentajes dentro de los segmentos
    arcs.append('text')
      .attr('transform', d => `translate(${arc.centroid(d)})`)
      .attr('dy', '0.35em')
      .attr('text-anchor', 'middle')
      .text(d => {
        const percentage = (d.data.count / totalTasks * 100);
        return percentage > 8 ? d3.format('.0%')(d.data.count / totalTasks) : '';
      })
      .style('fill', d => {
        // Determinar si el color es claro u oscuro para el texto
        const colorValue = color(d.data.status);
        const rgb = d3.rgb(colorValue);
        const brightness = (rgb.r * 299 + rgb.g * 587 + rgb.b * 114) / 1000;
        return brightness > 125 ? '#000000' : '#FFFFFF';
      })
      .style('font-size', '12px')
      .style('font-weight', 'bold')
      .style('pointer-events', 'none');

    // Leyenda a la derecha (si hay espacio)
    if (width > 400) {
      const legend = svg.append('g')
        .attr('class', 'legend')
        .attr('transform', `translate(${width/2 + radius + 20}, ${height/2 - (pieData.length * 25)/2})`);

      const legendItems = legend.selectAll('.legend-item')
        .data(pieData)
        .enter().append('g')
        .attr('class', 'legend-item')
        .attr('transform', (d, i) => `translate(0, ${i * 25})`)
        .style('cursor', 'pointer')
        .on('click', function(event, d) {
          const status = d.status;
          const filterElement = document.getElementById('status-filter');
          
          if (filterElement) {
            filterElement.value = filterElement.value === status ? '' : status;
            if (document.getElementById('user-filter')) {
              document.getElementById('user-filter').value = '';
            }
            renderGanttChart();
          }
        });

      legendItems.append('rect')
        .attr('width', 16)
        .attr('height', 16)
        .attr('fill', d => color(d.status))
        .style('rx', '3')
        .style('ry', '3');

      legendItems.append('text')
        .attr('x', 22)
        .attr('y', 11)
        .attr('dy', '0.35em')
        .text(d => `${d.label} (${d.count})`)
        .style('font-size', '12px')
        .style('font-family', 'Arial, sans-serif')
        .style('fill', '#333');
    }

    // Título
    svg.append('text')
      .attr('x', width / 2)
      .attr('y', 25)
      .attr('text-anchor', 'middle')
      .text('Distribución de Tareas por Estado')
      .style('font-size', '16px')
      .style('font-weight', 'bold')
      .style('fill', '#333')
      .style('font-family', 'Arial, sans-serif');

    // Total en el centro
    g.append('text')
      .attr('text-anchor', 'middle')
      .attr('dy', '-0.8em')
      .text('Total')
      .style('font-size', '14px')
      .style('fill', '#666')
      .style('font-weight', '500')
      .style('font-family', 'Arial, sans-serif');

    g.append('text')
      .attr('text-anchor', 'middle')
      .attr('dy', '0.8em')
      .text(totalTasks)
      .style('font-size', '24px')
      .style('font-weight', 'bold')
      .style('fill', '#333')
      .style('font-family', 'Arial, sans-serif');

    console.log('✅ renderProjectStatus: Gráfica renderizada exitosamente');
    
  } catch (error) {
    console.error('❌ Error en renderProjectStatus:', error);
    d3.select('#project-status-chart').html(`
      <div style="
        padding: 30px;
        text-align: center;
        color: #dc3545;
        background: #f8d7da;
        border-radius: 4px;
        border: 1px solid #f5c6cb;
        margin: 20px;
        font-family: Arial, sans-serif;
      ">
        <strong>Error cargando el gráfico:</strong><br>
        ${error.message}
      </div>
    `);
  }
}

function renderWorkloadLegend(container) {
  const relevantStatuses = {
    'TO_DO': statusColors['TO_DO'],
    'IN_PROGRESS': statusColors['IN_PROGRESS'],
    'BLOCKED': statusColors['BLOCKED'],
  };
  
  const legendData = Object.entries(relevantStatuses).map(([status, color]) => ({
    status: String(status).replace('_', ' '),
    color: color
  }));

  container.select('.workload-legend-container').remove();
  
  const legendDiv = container.append('div')
      .attr('class', 'workload-legend-container')
      .style('display', 'flex')
      .style('gap', '20px')
      .style('padding-top', '15px')
      .style('margin-top', '10px')
      .style('flex-wrap', 'wrap')
      .style('justify-content', 'center'); 
      
  legendData.forEach(d => {
    const item = legendDiv.append('div')
      .style('display', 'flex')
      .style('align-items', 'center')
      .style('gap', '5px')
      .style('font-size', '12px')
      .style('color', '#444');
      
    item.append('span')
      .style('width', '12px')
      .style('height', '12px')
      .style('border-radius', '3px')
      .style('background-color', d.color);
      
    item.append('span')
      .text(d.status);
  });
}

// =======================================================
// 3. CARGA DE TRABAJO (Bar Chart) - CORREGIDO
// =======================================================
async function renderWorkloadChart() {
  try {
    const response = await fetch(`${API_BASE_URL}/api/tasks/gantt`);
    const responseData = await response.json();
    
    let data = responseData.data || [];

    const container = d3.select('#workload-chart');
    container.html('');

    if (!Array.isArray(data) || data.length === 0) {
      container.append('div').attr('class', 'no-data-message')
        .text('No hay datos de tareas para la carga de trabajo.');
      renderWorkloadLegend(container); 
      return;
    }

    const statusKeys = Object.keys(statusColors).filter(s => s !== 'COMPLETED' && s !== 'CANCELLED');
    
    const dataByStatus = d3.group(
        data.filter(d => statusKeys.includes(getTaskStatus(d))), 
        d => String(d.assigned_user_id || 'N/A')
    );

    let processedData = Array.from(dataByStatus, ([raw_user_id, tasks]) => { 
      const display_user_id = raw_user_id === 'N/A' ? 'Sin Asignar' : `Usuario ${raw_user_id}`; 
      const userEntry = { 
        raw_user_id: raw_user_id, 
        display_user_id: display_user_id 
      };
      
      statusKeys.forEach(status => {
        userEntry[status] = tasks.filter(t => getTaskStatus(t) === status).length;
      });
      userEntry.total = Object.values(userEntry).reduce((sum, val) => typeof val === 'number' ? sum + val : sum, 0); 
      return userEntry;
    }).filter(d => d.total > 0).sort((a, b) => b.total - a.total);
    
    if (processedData.length === 0) {
      container.append('div').attr('class', 'no-data-message')
        .text('No hay tareas activas o pendientes asignadas.');
      renderWorkloadLegend(container);
      return;
    }
    
    const series = d3.stack().keys(statusKeys)(processedData);

    const margin = { top: 20, right: 60, bottom: 50, left: 200 }; 
    const containerWidth = container.node().clientWidth || 600;
    const width = Math.max(containerWidth - margin.left - margin.right, 400);

    const LEGEND_HEIGHT = 40; 
    const containerHeight = container.node().clientHeight || 600; 
    const height = containerHeight - margin.top - margin.bottom - LEGEND_HEIGHT; 
    const fallbackHeight = Math.max(processedData.length * 35, 200);
    const finalHeight = height > 0 ? height : fallbackHeight;

    const svg = container.append('svg')
      .attr('width', width + margin.left + margin.right)
      .attr('height', finalHeight + margin.top + margin.bottom) 
      .append('g')
      .attr('transform', `translate(${margin.left}, ${margin.top})`);

    const xMax = d3.max(processedData, d => d.total); 
    const x = d3.scaleLinear().domain([0, xMax]).range([0, width]);
    const y = d3.scaleBand().domain(processedData.map(d => d.display_user_id)).range([0, finalHeight]).padding(0.3);

    svg.append('g').attr('transform', `translate(0, ${finalHeight})`).call(d3.axisBottom(x).ticks(Math.min(10, xMax)).tickFormat(d3.format("d")))
      .selectAll('text').style('font-size', '14px'); 

    svg.append('g')
        .call(d3.axisLeft(y).tickFormat(d => d)) 
        .selectAll('text')
        .style('font-size', '14px') 
        .attr('text-anchor', 'end')
        .attr('dx', '-0.5em');

    svg.append("g")
      .selectAll("g")
      .data(series)
      .enter().append("g")
      .attr("fill", d => statusColors[d.key])
      .selectAll("rect")
      .data(d => d)
      .enter().append("rect")
      .attr("x", d => x(d[0]))
      .attr("y", d => y(d.data.display_user_id))
      .attr("height", y.bandwidth())
      .attr("width", d => x(d[1]) - x(d[0]))
      .style('cursor', 'pointer')
      .on('click', function (event, d) { 
        const rawUserId = d.data.raw_user_id;
        const filterElement = document.getElementById('user-filter');
        const statusFilterElement = document.getElementById('status-filter'); 
        
        if (filterElement) {
          const filterValue = rawUserId === 'N/A' ? '' : rawUserId; 
          
          if (filterElement.value === filterValue) {
              resetFiltersAndRefresh();
              return;
          }
          
          filterElement.value = filterValue;
          if(statusFilterElement) statusFilterElement.value = '';

          renderGanttChart();
        }
      })
      .on('mouseover', function (event, d) {
        const statusKey = d3.select(this.parentNode).datum().key;
        d3.select('#tooltip')
          .style('opacity', 1)
          .html(`<strong>${d.data.display_user_id}</strong><br>${statusKey.replace('_', ' ')}: ${d[1] - d[0]}`) 
          .style('left', (event.pageX + 10) + 'px')
          .style('top', (event.pageY - 28) + 'px');
      })
      .on('mouseout', function () {
        d3.select('#tooltip').style('opacity', 0);
      });

    svg.selectAll('.total-label')
      .data(processedData)
      .enter().append('text')
      .attr('class', 'total-label')
      .attr('x', d => x(d.total) + 6)
      .attr('y', d => y(d.display_user_id) + y.bandwidth() / 2)
      .attr('dy', '0.35em')
      .text(d => d.total)
      .style('font-size', '12px')
      .style('fill', '#111')
      .style('font-weight', 'bold');

    renderWorkloadLegend(container); 

    setTimeout(() => {
      const svgWidth = svg.node().parentElement.getBoundingClientRect().width;
      const containerWidthParent = container.node().parentElement.clientWidth;
      if (svgWidth > containerWidthParent) container.style('overflow-x', 'auto');
    }, 50);

  } catch (error) {
    console.error("Error al renderizar la carga de trabajo:", error);
    d3.select('#workload-chart').html(`<div class="error-message">Error cargando datos: ${error.message}</div>`);
  }
}

// =======================================================
// 4. TAREAS VENCIDAS (Overdue)
// =======================================================
async function renderOverdueTasks() {
  try {
    const response = await fetch(`${API_BASE_URL}/api/tasks/overdue`);
    const data = await response.json();

    const container = d3.select('#overdue-tasks');
    container.html('');

    if (!Array.isArray(data) || data.length === 0) {
      container.append('div').attr('class', 'no-data-message')
        .text('¡Excelente! No hay tareas vencidas.');
      return;
    }

    data.forEach(task => {
        const statusKey = getTaskStatus(task);
        const color = statusKey === 'TO_DO' || statusKey === 'IN_PROGRESS' || statusKey === 'BLOCKED' ? statusColors['TO_DO'] : statusColors['CANCELLED'];

        container.append('div')
            .attr('class', `task-item overdue`)
            .attr('style', `border-left: 5px solid ${color};`)
            .html(`
                <div style="display: flex; justify-content: space-between; align-items: center; width: 100%;">
                    <div style="font-weight: 600; font-size: 16px; flex-grow: 1;">
                        <i class="fas fa-exclamation-triangle" style="color: ${color};"></i>
                        <span class="task-title">${task.name || task.title || 'Tarea sin nombre'}</span>
                    </div>
                    
                    <div style="text-align:right; margin-left: 10px; min-width: 100px;">
                        <div class="user-badge" style="font-size: 14px; background-color: ${color}; color: white; padding: 2px 6px; border-radius: 4px; display: inline-block;">
                            Usuario ${task.assigned_user_id || task.assigned_to || 'N/A'}
                        </div>
                        <div class="task-date" style="margin-top:6px; font-size: 14px;">
                            Venció hace 
                            <span style="font-weight: bold; color: ${statusColors['TO_DO']}; display: block;">${task.days_overdue || '?'} días</span>
                        </div>
                    </div>
                </div>
            `);
    });

  } catch (error) {
    console.error("Error al renderizar tareas vencidas:", error);
    d3.select('#overdue-tasks').html(`<div class="error-message">Error cargando datos: ${error.message}</div>`);
  }
}

// =======================================================
// 5. SCOREBOARD DE EFICIENCIA POR RECURSO - CORREGIDO
// =======================================================
async function calculateAndRenderEfficiencyScoreboard() {
    try {
        const response = await fetch(`${API_BASE_URL}/api/tasks/gantt`);
        const responseData = await response.json();
        
        const data = responseData.data || [];
        
        const container = d3.select('#efficiency-scoreboard');
        container.html('');
        
        container.style('height', 'auto');

        if (!Array.isArray(data) || data.length === 0) {
            container.append('div').attr('class', 'no-data-message')
                .text('No hay datos de tareas para el Scoreboard.');
            return;
        }

        const today = new Date();
        today.setHours(0, 0, 0, 0); 

        const scoreboardDataMap = new Map();
        
        data.forEach(task => {
            const userId = String(task.assigned_user_id || 'N/A').trim();
            if (userId === '') return; 

            if (!scoreboardDataMap.has(userId)) {
                scoreboardDataMap.set(userId, {
                    user_id: userId,
                    total_tasks: 0,
                    completed_tasks: 0,
                    overdue_tasks: 0
                });
            }

            const userStats = scoreboardDataMap.get(userId);
            const statusKey = getTaskStatus(task);

            if (statusKey !== 'CANCELLED') {
                userStats.total_tasks += 1;
            }

            if (statusKey === 'COMPLETED') {
                userStats.completed_tasks += 1;
            }

            if (statusKey !== 'COMPLETED' && statusKey !== 'CANCELLED') {
                const dueDate = task.end_date || task.due_date;
                const parsedDate = parseDateFlexible(dueDate);

                if (parsedDate && parsedDate < today) {
                    userStats.overdue_tasks += 1;
                }
            }
        });

        const finalData = Array.from(scoreboardDataMap.values())
            .filter(d => d.total_tasks > 0)
            .map(d => ({
                ...d,
                completion_rate: d.total_tasks > 0 ? (d.completed_tasks / d.total_tasks) * 100 : 0
            }))
            .sort((a, b) => {
                if (b.completion_rate !== a.completion_rate) return b.completion_rate - a.completion_rate;
                if (a.overdue_tasks !== b.overdue_tasks) return a.overdue_tasks - b.overdue_tasks;
                return a.user_id.localeCompare(b.user_id);
            });

        if (finalData.length === 0) {
            container.append('div').attr('class', 'no-data-message')
                .text('No hay tareas para calcular la eficiencia.');
            return;
        }

        const table = container.append('table').attr('class', 'efficiency-table');

        table.append('thead').append('tr')
            .html(`
                <th>Recurso (Usuario)</th>
                <th style="text-align:center;">Tareas Asignadas</th>
                <th style="text-align:center;">Finalizadas</th>
                <th style="text-align:center;">Tasa de Finalización</th>
                <th style="text-align:center;">Tareas Vencidas</th>
            `);

        const tbody = table.append('tbody');

        finalData.forEach(d => {
            const completionRate = d.completion_rate;
            let rateColor = '#333';
            if (completionRate === 100) rateColor = statusColors['COMPLETED'];
            else if (completionRate < 50) rateColor = statusColors['TO_DO'];
            else if (completionRate >= 50) rateColor = statusColors['IN_PROGRESS'];
            
            let overdueColor = '#333';
            if (d.overdue_tasks > 0) overdueColor = statusColors['TO_DO'];
            else overdueColor = statusColors['COMPLETED'];

            const row = tbody.append('tr');

            row.append('td')
                .text(`Usuario ${d.user_id === 'N/A' ? 'Sin Asignar' : d.user_id}`)
                .style('font-weight', 'bold');
            row.append('td')
                .text(d.total_tasks)
                .style('text-align', 'center')
                .style('font-size', '16px')
                .style('font-weight', 'bold'); 
            row.append('td')
                .text(d.completed_tasks)
                .style('text-align', 'center')
                .style('font-size', '16px')
                .style('font-weight', 'bold');
            row.append('td')
                .text(`${completionRate.toFixed(1)}%`)
                .style('color', rateColor)
                .style('font-weight', 'bold')
                .style('font-size', '16px')
                .style('text-align', 'center');
            row.append('td')
                .text(d.overdue_tasks)
                .style('color', overdueColor)
                .style('font-weight', 'bold')
                .style('font-size', '16px')
                .style('text-align', 'center');
        });
    } catch (error) {
        console.error("Error al calcular y renderizar el Scoreboard de Eficiencia:", error);
        d3.select('#efficiency-scoreboard').html(`<div class="error-message">Error cargando datos: ${error.message}</div>`);
    }
}

// =======================================================
// 6. TAREAS PRÓXIMAS - CORREGIDO
// =======================================================
function getRelativeDate(date) {
  const today = new Date();
  today.setHours(0, 0, 0, 0);
  const target = new Date(date);
  target.setHours(0, 0, 0, 0);
  const diffTime = target.getTime() - today.getTime();
  const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));

  if (diffDays === 0) return 'Hoy';
  if (diffDays === 1) return 'Mañana';
  if (diffDays > 1 && diffDays <= 7) return `En ${diffDays} días`; 
  if (diffDays > 7) return 'Más adelante';
  return 'N/A';
}

async function renderDailyTasks() {
  try {
    const response = await fetch(`${API_BASE_URL}/api/tasks/gantt`);
    const responseData = await response.json();
    
    const data = responseData.data || [];

    const container = d3.select('#daily-tasks');
    container.html('');

    const today = new Date();
    today.setHours(0, 0, 0, 0);

    const upcomingTasks = data.filter(task => {
        const statusKey = getTaskStatus(task);
        if (statusKey === 'COMPLETED' || statusKey === 'CANCELLED') return false;

        const dueDate = task.end_date || task.due_date;
        const parsedDate = parseDateFlexible(dueDate);

        return parsedDate && parsedDate >= today; 
    });
    
    if (upcomingTasks.length === 0) {
      container.append('div').attr('class', 'no-data-message')
        .text('No hay tareas próximas pendientes.');
      return;
    }

    upcomingTasks.sort((a, b) => {
        const dateA = parseDateFlexible(a.end_date || a.due_date);
        const dateB = parseDateFlexible(b.end_date || b.due_date);
        return (dateA?.getTime() || Infinity) - (dateB?.getTime() || Infinity);
    });
    
    const limitedTasks = upcomingTasks.slice(0, 10);

    limitedTasks.forEach(task => {
        const dueDate = parseDateFlexible(task.end_date || task.due_date);
        const relativeDate = dueDate ? getRelativeDate(dueDate) : 'Sin Fecha';
        const user = task.assigned_user_id || task.assigned_to || 'N/A';
        const taskTitle = task.name || task.title || 'Tarea sin nombre';
        const color = statusColors[getTaskStatus(task)] || '#cccccc';

        container.append('div')
            .attr('class', `task-item upcoming`)
            .attr('style', `border-left: 5px solid ${color};`)
            .html(`
                <div style="flex-grow: 1;">
                    <div style="font-weight: 600; font-size: 16px; margin-bottom: 5px;">
                        <i class="fas fa-calendar-check" style="color: ${color};"></i>
                        <span class="task-title">${taskTitle}</span>
                    </div>
                    <div class="user-badge" style="background-color: ${color}; color: white; padding: 2px 6px; border-radius: 4px; display: inline-block; font-size: 11px;">
                        Usuario ${user}
                    </div>
                </div>
                <div style="text-align:right; margin-left: 10px; min-width: 80px;">
                    <div class="task-date" style="font-size: 14px; font-weight: bold; color: ${color};">
                        ${relativeDate}
                    </div>
                    <div style="font-size: 12px; color: #666;">
                        (${dueDate ? dueDate.toLocaleDateString() : 'N/A'})
                    </div>
                </div>
            `);
    });

  } catch (error) {
    console.error("Error al renderizar tareas próximas:", error);
    d3.select('#daily-tasks').html(`<div class="error-message">Error cargando datos: ${error.message}</div>`);
  }
}

// =======================================================
// 7. MÉTRICAS (KPIs)
// =======================================================
async function renderMetrics() {
  try {
    const response = await fetch(`${API_BASE_URL}/api/metrics`);
    const data = await response.json();

    d3.select('#total-tasks').text(data.total_tasks || 0);
    d3.select('#completed-tasks').text(data.completed_tasks || 0);
    d3.select('#completion-rate').text(d3.format(".1f")(data.completion_rate || 0) + '%');
    d3.select('#avg-completion-time').text(typeof data.avg_completion_time === 'number' ? `${data.avg_completion_time.toFixed(1)} días` : 'N/A');
  } catch (error) {
    console.error("Error al renderizar métricas:", error);
    d3.select('#total-tasks').text('N/A');
    d3.select('#completed-tasks').text('N/A');
    d3.select('#completion-rate').text('N/A');
    d3.select('#avg-completion-time').text('N/A');
  }
}

// =======================================================
// 8. GANTT (con filtros) - CORREGIDO
// =======================================================

/**
 * Obtiene los usuarios únicos del endpoint /api/tasks/gantt y puebla el filtro.
 */
async function loadGanttFilters() {
    try {
        const response = await fetch(`${API_BASE_URL}/api/tasks/gantt`);
        const responseData = await response.json();
        
        const tasks = responseData.data || [];
        
        const userIds = new Set();
        tasks.forEach(task => {
            const userId = String(task.assigned_user_id || 'N/A').trim();
            if (userId !== 'N/A' && userId !== '') {
                userIds.add(userId);
            }
        });
        
        const sortedUserIds = Array.from(userIds).sort();

        const userFilterSelect = d3.select('#user-filter');
        const currentFilterValue = userFilterSelect.node() ? userFilterSelect.node().value : '';
        
        userFilterSelect.html('<option value="">Todos los usuarios</option>');
        userFilterSelect.append('option')
          .attr('value', 'N/A')
          .text('Sin Asignar');

        sortedUserIds.forEach(id => {
            userFilterSelect.append('option')
                .attr('value', id)
                .text(`Usuario ${id}`);
        });

        if (currentFilterValue && (sortedUserIds.includes(currentFilterValue) || currentFilterValue === 'N/A')) {
             userFilterSelect.property('value', currentFilterValue);
        }

    } catch (error) {
        console.error("Error cargando filtros de usuario desde /api/tasks/gantt:", error);
    }
}

function parseGanttDate(dateString) {
    if (!dateString) return null;
    let date = new Date(String(dateString).trim());
    return isNaN(date.getTime()) ? null : date;
}

async function renderGanttChart() {
  try {
    const statusFilter = document.getElementById('status-filter')?.value || '';
    const userFilter = document.getElementById('user-filter')?.value || '';
    
    const params = new URLSearchParams();
    if (statusFilter) params.append('status', statusFilter);
    if (userFilter) params.append('user_id', userFilter);

    const apiUrl = `${API_BASE_URL}/api/tasks/gantt?${params.toString()}`;
    const response = await fetch(apiUrl);
    const responseData = await response.json();
    
    const rawData = responseData.data || [];

    const container = d3.select('#gantt-chart');
    container.html('<div id="gantt-diagnostic"></div>');
    
    const data = rawData.map(d => {
        const start = parseGanttDate(d.start_date);
        const end = parseGanttDate(d.end_date || d.due_date);
        
        const userId = String(d.assigned_user_id || 'N/A');
        const userName = userId === 'N/A' ? 'Sin Asignar' : `Usuario ${userId}`;
        const taskName = d.name || d.title || 'Tarea sin nombre';
        
        let durationDays = 'N/A';
        if (start && end) {
            const diffTime = Math.abs(end.getTime() - start.getTime());
            durationDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24)); 
        }

        return {
            ...d,
            start: start,
            end: end,
            task_id: String(d._id || d.id || d.task_id || Math.random()),
            user_id: userId,
            task_name: taskName,
            task_label: `${taskName} [${userName}]`,
            duration_days: durationDays 
        };
    }).filter(d => d.start && d.end && d.start < d.end);

    if (data.length === 0) { 
      container.append('div').attr('class', 'no-data-message')
        .text('No hay tareas con fechas de inicio y fin válidas para los filtros seleccionados.');
      return;
    }

    const margin = { top: 25, right: 30, bottom: 60, left: 300 }; 
    const parentWidth = container.node().parentElement.clientWidth || 1400;
    const width = Math.max(parentWidth - margin.left - margin.right, 1200);
    const height = Math.max(data.length * 45, 400);

    d3.select('#gantt-chart').select('svg').remove();
    
    const svg = container.append('svg')
      .attr('width', width + margin.left + margin.right)
      .attr('height', height + margin.top + margin.bottom)
      .append('g')
      .attr('transform', `translate(${margin.left}, ${margin.top})`);

    const dateRange = [d3.min(data, d => d.start), d3.max(data, d => d.end)];
    let datePadding = 2, tickFormat = d3.timeFormat("%a %d"), tickCount = Math.ceil(width / 70);

    switch (ganttZoomLevel) {
        case 'weeks':
            datePadding = 14;
            tickFormat = d3.timeFormat("Sem %U\n%b %d");
            tickCount = Math.ceil(width / 100);
            break;
        case 'months':
            datePadding = 60;
            tickFormat = d3.timeFormat("%b %Y");
            tickCount = Math.ceil(width / 120);
            break;
        default:
            datePadding = 2;
            tickFormat = d3.timeFormat("%a %d");
            tickCount = Math.ceil(width / 70);
    }
    
    dateRange[0] = d3.timeDay.offset(d3.timeDay.floor(dateRange[0]), -datePadding);
    dateRange[1] = d3.timeDay.offset(d3.timeDay.ceil(dateRange[1]), datePadding);
    
    const x = d3.scaleTime().domain(dateRange).range([0, width]);
    const y = d3.scaleBand().domain(data.map(d => d.task_label)).range([0, height]).padding(0.2);
    
    const barColor = status => statusColors[String(status).toUpperCase()] || '#cccccc';

    svg.selectAll('.task-bar')
      .data(data)
      .enter().append('rect')
      .attr('class', 'task-bar')
      .attr('y', d => y(d.task_label))
      .attr('height', y.bandwidth())
      .attr('x', d => Math.max(0, x(d.start)))
      .attr('width', d => {
        const startX = Math.max(0, x(d.start));
        const endX = x(d.end);
        return Math.max(0, endX - startX);
      })
      .attr('fill', d => barColor(getTaskStatus(d))) 
      .on('mouseover', function (event, d) {
        d3.select('#tooltip')
          .style('opacity', 1)
          .html(`
            <strong>${d.task_name}</strong><br>
            Usuario: ${d.user_id === 'N/A' ? 'Sin Asignar' : d.user_id}<br>
            Estado: ${String(getTaskStatus(d)).replace('_', ' ')}<br>
            Inicio: ${d.start ? d3.timeFormat("%d/%b")(d.start) : 'N/A'}<br>
            Fin: ${d.end ? d3.timeFormat("%d/%b")(d.end) : 'N/A'}<br>
            <strong>Duración: ${d.duration_days} días</strong>
          `) 
          .style('left', (event.pageX + 10) + 'px')
          .style('top', (event.pageY - 28) + 'px');
      })
      .on('mouseout', function () {
        d3.select('#tooltip').style('opacity', 0);
      });

    svg.selectAll('.task-label-inside')
      .data(data)
      .enter().append('text')
      .attr('class', 'task-label-inside')
      .attr('x', d => {
        const barStart = Math.max(0, x(d.start));
        return barStart + 5;
      })
      .attr('y', d => y(d.task_label) + y.bandwidth() / 2)
      .attr('dy', '.35em') 
      .text(d => d.task_name)
      .attr('fill', '#ffffff') 
      .style('font-size', '12px')
      .style('font-weight', 'bold')
      .style('pointer-events', 'none') 
      .style('display', d => {
        const barStart = Math.max(0, x(d.start));
        const barEnd = x(d.end);
        const barWidth = Math.max(0, barEnd - barStart);
        return barWidth > 120 ? null : 'none'; 
      });

    svg.append('g')
      .attr('class', 'x-axis')
      .attr('transform', `translate(0, ${height})`)
      .call(d3.axisBottom(x).ticks(tickCount).tickFormat(tickFormat))
      .selectAll("text")
      .style("text-anchor", "end")
      .attr("dx", "-.8em")
      .attr("dy", ".15em")
      .attr("transform", "rotate(-65)");

    svg.append('g')
      .attr('class', 'y-axis')
      .call(d3.axisLeft(y).tickFormat(d => d));

    const today = new Date();
    today.setHours(0, 0, 0, 0);

    if (x.domain()[0] <= today && x.domain()[1] >= today) {
      const todayX = x(today);
      const todayFormatted = d3.timeFormat("%d %b")(today);

      svg.append('line')
        .attr('class', 'today-line')
        .attr('x1', todayX)
        .attr('x2', todayX)
        .attr('y1', 0)
        .attr('y2', height)
        .style('stroke', '#ff0ac4') 
        .style('stroke-width', '2px')
        .style('stroke-dasharray', '4');
        
      svg.append('text')
        .attr('class', 'today-text-label')
        .attr('x', todayX - 5)
        .attr('y', -5) 
        .attr('text-anchor', 'end')
        .text('HOY')
        .style('fill', '#ff0ac4')
        .style('font-weight', 'bold')
        .style('font-size', '16px'); 

      svg.append('text')
        .attr('class', 'today-text-date')
        .attr('x', todayX + 5)
        .attr('y', -5) 
        .attr('text-anchor', 'start')
        .text(todayFormatted)
        .style('fill', '#ff0ac4')
        .style('font-weight', 'bold') 
        .style('font-size', '16px'); 
    }

    const errors = data.filter(d => getTaskStatus(d) !== 'COMPLETED' && d.end < today);

    if (errors.length > 0) {
      d3.select('#gantt-diagnostic').html(`<i class="fas fa-exclamation-triangle" style="color: ${statusColors['TO_DO']};"></i> <strong>${errors.length} tareas</strong> vencidas y no completadas.`);
    } else {
      d3.select('#gantt-diagnostic').html(`<i class="fas fa-check-circle" style="color: ${statusColors['COMPLETED']};"></i> No hay tareas vencidas en esta vista.`);
    }

    setTimeout(() => {
      const svgWidth = svg.node().parentElement.getBoundingClientRect().width;
      const containerWidthParent = container.node().parentElement.clientWidth;
      if (svgWidth > containerWidthParent) container.style('overflow-x', 'auto');
    }, 50);

  } catch (error) {
    console.error("Error al renderizar el Gantt:", error);
    d3.select('#gantt-chart').html(`<div class="error-message">Error cargando el diagrama de Gantt. Mensaje: ${error.message}</div>`);
  }
}

// =======================================================
// 9. EVENTO INICIAL Y REFRESH
// =======================================================

function loadAllData() {
    const loadingDiv = document.getElementById('loading-message');
    if (loadingDiv) loadingDiv.style.display = 'block';

    const tasks = [
      renderMetrics(),
      renderProjectStatus(),
      renderWorkloadChart(),
      renderOverdueTasks(),
      calculateAndRenderEfficiencyScoreboard(), 
      renderDailyTasks(),
      loadGanttFilters().then(() => renderGanttChart())
    ];
    
    return Promise.all(tasks).finally(() => {
      const loadingDiv = document.getElementById('loading-message');
      lastUpdateTime = new Date();
      d3.select('#last-update-time').text(lastUpdateTime.toLocaleTimeString());
      if (loadingDiv) loadingDiv.style.display = 'none';
    });
}

// Inicialización
document.addEventListener('DOMContentLoaded', () => {
    if (!document.getElementById('tooltip')) {
        d3.select('body').append('div')
            .attr('id', 'tooltip')
            .attr('class', 'tooltip')
            .style('position', 'absolute')
            .style('opacity', 0)
            .style('background-color', 'rgba(255, 255, 255, 0.95)')
            .style('border', '1px solid #ddd')
            .style('padding', '8px')
            .style('border-radius', '4px')
            .style('pointer-events', 'none')
            .style('z-index', 99999); 
    }

    const ingestBtn = document.getElementById('ingest-csv-btn');
    if (ingestBtn) {
        ingestBtn.addEventListener('click', handleIngestCsv);
    }

    const statusFilter = document.getElementById('status-filter');
    if (statusFilter) statusFilter.addEventListener('change', renderGanttChart);

    const userFilter = document.getElementById('user-filter');
    if (userFilter) userFilter.addEventListener('change', renderGanttChart);
    
    const refreshBtn = document.getElementById('refresh-btn');
    if (refreshBtn) refreshBtn.addEventListener('click', () => { if (typeof loadAllData === 'function') loadAllData(); });
    
    zoomGantt(ganttZoomLevel);
    loadAllData();
});
