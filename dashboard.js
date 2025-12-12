// =======================================================
// CONFIGURACIÓN GLOBAL Y HELPERS
// =======================================================
const API_BASE_URL = 'https://pm-dashb-7.onrender.com';

let lastUpdateTime = null;
let ganttZoomLevel = 'weeks'; // 'days' | 'weeks' | 'months' (Cambiado a 'weeks' por defecto)

// Paleta de colores para estados
const statusColors = {
  'TO_DO': '#FF0AC4',     // Rosa fuerte (Alerta/Pendiente)
  'IN_PROGRESS': '#50F8FA', // Cian (Trabajo activo)
  'BLOCKED': '#FFCD00',   // Amarillo (Advertencia) - CORREGIDO
  'COMPLETED': '#27E568', // Verde (Éxito)
  'CANCELLED': '#555555'  // Gris oscuro (Neutro) - CORREGIDO
};

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

// Helper para parsear la fecha del Gantt (permite la hora si es necesario)
function parseGanttDate(dateString) {
    if (!dateString) return null;
    let date = new Date(String(dateString).trim());
    return isNaN(date.getTime()) ? null : date;
}

// Helper robusto para obtener el estado de una tarea (uso en Workload, Overdue, Gantt)
const getTaskStatus = (task) => String(task.status || 'TO_DO').toUpperCase();

// Helpers robustos para el Donut/Pie Chart
const getPieStatus = (d) => String(d.data.status || d.data.name || 'TO_DO').toUpperCase();
const getPieLabel = (d) => String(d.data.status || d.data.name || 'Indefinido').replace('_', ' ');

// =======================================================
// 1. CARGA Y SOBREESCRITURA DE CSV (sin cambios)
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
// 2. ESTADO DEL PROYECTO (Donut Chart) - CORREGIDO
// La lógica de renderizado de texto interno incorrecto ha sido ELIMINADA.
// =======================================================
async function renderProjectStatus() {
  try {
    const response = await fetch(`${API_BASE_URL}/api/project/status`);
    const responseData = await response.json();
    
    // Extraer array 'projects' del objeto
    const data = responseData.projects || [];

    const container = d3.select('#project-status-chart');
    container.html('');

    if (!Array.isArray(data) || data.length === 0) {
      container.append('div').attr('class', 'no-data-message')
        .text('No hay datos de tareas para el estado del proyecto.');
      return;
    }

    // Cálculo del total para el porcentaje (¡CRÍTICO!)
    const totalTasks = data.reduce((sum, d) => sum + (Number(d.total_tasks) || 0), 0);
    const width = 450, height = 300, outerRadius = 120, innerRadius = outerRadius * 0.6;

    const svg = container.append('svg')
      .attr('width', width)
      .attr('height', height)
      .append('g')
      .attr('transform', `translate(${outerRadius + 20}, ${height / 2})`);

    // Preparar datos para el gráfico de donut
    const pieData = [];
    data.forEach(project => {
      if (project.statuses && Array.isArray(project.statuses)) {
        project.statuses.forEach(status => {
          pieData.push({
            _id: `${project.project} - ${status.status}`,
            status: status.status,
            count: status.count,
            name: status.status
          });
        });
      }
    });

    if (pieData.length === 0) {
      container.append('div').attr('class', 'no-data-message')
        .text('No hay datos de estados para mostrar.');
      return;
    }

    const pie = d3.pie().value(d => d.count).sort(null);
    const arc = d3.arc().innerRadius(innerRadius).outerRadius(outerRadius);

    const arcs = svg.selectAll('.arc')
      .data(pie(pieData))
      .enter().append('g')
      .attr('class', 'arc');

    arcs.append('path')
      .attr('d', arc)
      .attr('fill', d => statusColors[getPieStatus(d)] || '#cccccc')
      .style('cursor', 'pointer')
      .on('click', function (event, d) {
        const status = getPieStatus(d);
        const filterElement = document.getElementById('status-filter');
        const userFilterElement = document.getElementById('user-filter'); 

        if (filterElement) {
          if (filterElement.value === status) {
              resetFiltersAndRefresh();
              return;
          }
          filterElement.value = status;
          if(userFilterElement) userFilterElement.value = '';
          
          renderGanttChart();
        }
      })
      .on('mouseover', function (event, d) {
        d3.select('#tooltip')
          .style('opacity', 1)
          .html(`<strong>${getPieLabel(d)}</strong>: ${d.data.count} tareas (${d3.format(".1%")(d.data.count / Math.max(1, totalTasks))})`)
          .style('left', (event.pageX + 10) + 'px')
          .style('top', (event.pageY - 28) + 'px');
      })
      .on('mouseout', function () {
        d3.select('#tooltip').style('opacity', 0);
      });

    // El bloque comentado que causaba porcentajes erróneos está CORRECTO (debe permanecer comentado/eliminado)

    renderLegend(svg, pieData, totalTasks, outerRadius);
  } catch (error) {
    console.error("Error al renderizar el estado del proyecto:", error);
    d3.select('#project-status-chart').html(`<div class="error-message">Error cargando datos: ${error.message}</div>`);
  }
}

function renderLegend(svg, data, totalTasks, outerRadius) {
  const legendOffset = outerRadius + 50;
  const legendSpacing = 20;

  // Agrupar por estado para la leyenda (¡Correcto! Aquí se consolida el 100%)
  const statusCounts = {};
  data.forEach(d => {
    const status = d.status; // Usar d.status, ya que pieData ya lo tiene
    if (!statusCounts[status]) statusCounts[status] = 0;
    statusCounts[status] += d.count;
  });

  const legendData = Object.keys(statusCounts).map(status => ({
    _id: status,
    count: statusCounts[status]
  }));

  const legend = svg.selectAll(".legend")
    .data(legendData)
    .enter().append("g")
    .attr("class", "legend")
    .attr("transform", (d, i) => `translate(${legendOffset}, ${i * legendSpacing - (legendData.length * legendSpacing) / 2 + 10})`);

  legend.append("rect")
    .attr("x", 0)
    .attr("width", 10)
    .attr("height", 10)
    .attr('fill', d => statusColors[String(d._id).toUpperCase()] || '#888888');

  legend.append("text")
    .attr("x", 15)
    .attr("y", 9)
    .attr("dy", ".35em")
    .style("text-anchor", "start")
    .style("font-size", "12px")
    .text(d => `${String(d._id || 'Indefinido').replace('_', ' ')} - ${((d.count / Math.max(1, totalTasks)) * 100).toFixed(1)}%`);
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
    
    // EXTRAER 'data' del objeto responseData
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
// 4. TAREAS VENCIDAS (Overdue) (sin cambios)
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
        
        // EXTRAER 'data' del objeto responseData
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
    
    // EXTRAER 'data' del objeto responseData
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
// 7. MÉTRICAS (KPIs) (sin cambios)
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
// 8. GANTT (con filtros) - COMPLETO Y CORREGIDO
// =======================================================

/**
 * Obtiene los usuarios únicos del endpoint /api/tasks/gantt y puebla el filtro.
 */
async function loadGanttFilters() {
    try {
        const response = await fetch(`${API_BASE_URL}/api/tasks/gantt`);
        const responseData = await response.json();
        
        // EXTRAER 'data' del objeto responseData
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
    
    // EXTRAER 'data' del objeto responseData
    const rawData = responseData.data || [];

    const container = d3.select('#gantt-chart');
    container.html(''); // Limpiar el contenedor anterior

    // 1. Mapeo y filtrado de datos
    const data = rawData.map(d => {
        const start = parseGanttDate(d.start_date);
        const end = parseGanttDate(d.end_date || d.due_date);
        
        // La duración en milisegundos
        const durationMs = (end && start) ? end.getTime() - start.getTime() : 0;
        // La duración en días (aproximada)
        const durationDays = durationMs > 0 ? Math.ceil(durationMs / (1000 * 60 * 60 * 24)) : 0;

        const userId = String(d.assigned_user_id || 'N/A');
        const userName = userId === 'N/A' ? 'Sin Asignar' : `Usuario ${userId}`;
        const taskName = d.name || d.title || 'Tarea sin nombre';
        
        return {
            id: d.task_id,
            name: taskName,
            user: userName,
            start: start,
            end: end,
            status: getTaskStatus(d),
            duration: durationDays
        };
    }).filter(d => d.start && d.end && d.end >= d.start); // Solo tareas válidas

    if (data.length === 0) {
      container.append('div').attr('class', 'no-data-message')
        .text('No hay tareas que cumplan con los filtros y tengan fechas válidas.');
      return;
    }

    // Ordenar tareas: por usuario y luego por fecha de inicio
    data.sort((a, b) => {
        if (a.user !== b.user) return a.user.localeCompare(b.user);
        return a.start.getTime() - b.start.getTime();
    });
    
    // 2. Configuración de dimensiones y márgenes
    const margin = { top: 20, right: 20, bottom: 30, left: 150 };
    const containerWidth = container.node().clientWidth || 900;
    const barHeight = 25;
    const barPadding = 5;
    const rowHeight = barHeight + barPadding;
    const chartHeight = data.length * rowHeight;
    const width = containerWidth - margin.left - margin.right;
    const height = Math.max(chartHeight, 200);

    // 3. Crear SVG
    const svg = container.append('svg')
      .attr('width', width + margin.left + margin.right)
      .attr('height', height + margin.top + margin.bottom)
      .append('g')
      .attr('transform', `translate(${margin.left}, ${margin.top})`);
    
    // 4. Escalas
    const minDate = d3.min(data, d => d.start);
    const maxDate = d3.max(data, d => d.end);
    
    // Extender el dominio de tiempo para tener un margen visual
    const domainStart = d3.timeDay.offset(minDate, -1);
    const domainEnd = d3.timeDay.offset(maxDate, 1);
    
    const scaleX = d3.scaleTime()
      .domain([domainStart, domainEnd])
      .range([0, width]);

    const scaleY = d3.scaleBand()
      .domain(data.map(d => d.id)) // Usamos el ID como dominio Y
      .range([0, height])
      .paddingInner(barPadding / rowHeight);

    // 5. Ejes
    // Determinar el formato del eje X basado en el nivel de zoom
    let xAxisFormat, tickInterval;
    switch (ganttZoomLevel) {
        case 'days':
            xAxisFormat = d3.timeFormat("%b %d");
            tickInterval = d3.timeDay;
            break;
        case 'weeks':
            xAxisFormat = d3.timeFormat("%Y/%W"); // Semana del año
            tickInterval = d3.timeWeek;
            break;
        case 'months':
        default:
            xAxisFormat = d3.timeFormat("%b %Y");
            tickInterval = d3.timeMonth;
            break;
    }

    const xAxis = d3.axisBottom(scaleX)
      .tickFormat(xAxisFormat)
      .tickSize(6)
      .ticks(tickInterval);

    svg.append('g')
      .attr('class', 'x-axis')
      .attr('transform', `translate(0, ${height})`)
      .call(xAxis)
      .selectAll("text")
      .attr("transform", "rotate(-30)")
      .style("text-anchor", "end");

    // Eje Y (Etiquetas de tareas y usuarios)
    const yAxis = d3.axisLeft(scaleY)
      .tickFormat(id => {
         const task = data.find(d => d.id === id);
         return `${task.name} (${task.user})`;
      })
      .tickSize(0);

    svg.append('g')
      .attr('class', 'y-axis')
      .call(yAxis)
      .selectAll("text")
      .style("font-size", "12px");
    
    // 6. Dibujo de las Barras de Tareas
    const bars = svg.selectAll('.task-bar')
      .data(data)
      .enter().append('rect')
      .attr('class', 'task-bar')
      .attr('x', d => scaleX(d.start))
      .attr('y', d => scaleY(d.id))
      .attr('width', d => scaleX(d.end) - scaleX(d.start))
      .attr('height', scaleY.bandwidth())
      .attr('fill', d => statusColors[d.status] || '#cccccc')
      .attr('rx', 3) // bordes redondeados
      .attr('ry', 3)
      .style('cursor', 'pointer')
      .on('mouseover', function (event, d) {
        // Tooltip al pasar el ratón
        d3.select('#tooltip')
          .style('opacity', 1)
          .html(`
            <strong>${d.name}</strong><br>
            Recurso: ${d.user}<br>
            Estado: ${d.status.replace('_', ' ')}<br>
            Inicio: ${d3.timeFormat("%Y-%m-%d")(d.start)}<br>
            Fin: ${d3.timeFormat("%Y-%m-%d")(d.end)} (${d.duration} días)
          `)
          .style('left', (event.pageX + 10) + 'px')
          .style('top', (event.pageY - 28) + 'px');
      })
      .on('mouseout', function () {
        d3.select('#tooltip').style('opacity', 0);
      });

    // 7. Indicador de "Hoy" (Línea de Referencia Vertical)
    const today = new Date();
    today.setHours(0, 0, 0, 0);
    
    if (scaleX(today) >= 0 && scaleX(today) <= width) {
        svg.append('line')
            .attr('class', 'today-line')
            .attr('x1', scaleX(today))
            .attr('x2', scaleX(today))
            .attr('y1', 0)
            .attr('y2', height)
            .style('stroke', 'red')
            .style('stroke-width', 2)
            .style('stroke-dasharray', '5,5');

        svg.append('text')
            .attr('class', 'today-label')
            .attr('x', scaleX(today))
            .attr('y', -5)
            .style('fill', 'red')
            .style('font-size', '12px')
            .style('text-anchor', 'middle')
            .text('HOY');
    }

    // Habilitar scroll horizontal si el contenido es demasiado ancho
    if (width + margin.left + margin.right > containerWidth) {
      container.style('overflow-x', 'auto');
    }

  } catch (error) {
    console.error("Error al renderizar el Diagrama de Gantt:", error);
    d3.select('#gantt-chart').html(`<div class="error-message">Error cargando el Gantt: ${error.message}</div>`);
  }
}


// =======================================================
// 9. FUNCIÓN DE CARGA INICIAL
// =======================================================
async function loadAllData() {
  console.log("Cargando todos los datos del dashboard...");
  try {
    // Carga las dependencias del Gantt primero
    await loadGanttFilters(); 
    await renderGanttChart();

    // Carga el resto del dashboard en paralelo
    await Promise.all([
      renderMetrics(),
      renderProjectStatus(),
      renderWorkloadChart(),
      renderOverdueTasks(),
      renderDailyTasks(),
      calculateAndRenderEfficiencyScoreboard()
    ]);
    
    lastUpdateTime = new Date().toLocaleTimeString();
    d3.select('#last-update-time').text(lastUpdateTime);
    console.log("✅ Carga de datos inicial completa.");
  } catch (error) {
    console.error("Fallo durante la carga inicial del dashboard:", error);
  }
}

// 10. INICIALIZACIÓN
document.addEventListener('DOMContentLoaded', () => {
    loadAllData();
    
    // Asignar listeners a los filtros del Gantt (si existen)
    const statusFilter = document.getElementById('status-filter');
    const userFilter = document.getElementById('user-filter');

    if (statusFilter) statusFilter.addEventListener('change', renderGanttChart);
    if (userFilter) userFilter.addEventListener('change', renderGanttChart);

    // Listener para la ingesta de CSV
    const ingestButton = document.getElementById('ingest-csv-button');
    if (ingestButton) ingestButton.addEventListener('click', handleIngestCsv);
    
    // Inicializar Tooltip global de D3
    d3.select('body').append('div')
        .attr('id', 'tooltip')
        .style('opacity', 0)
        .style('position', 'absolute')
        .style('padding', '8px')
        .style('background', 'rgba(0, 0, 0, 0.7)')
        .style('border-radius', '4px')
        .style('color', 'white')
        .style('pointer-events', 'none');

    // Inicializar el estado de zoom (por si la página no lo hace)
    zoomGantt('weeks');
});
