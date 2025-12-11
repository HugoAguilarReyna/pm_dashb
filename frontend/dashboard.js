// dashboard.js - VERSIÓN FINAL CON SCOREBOARD Y TAREAS PRÓXIMAS (SIN LÍMITE DE 30 DÍAS)

// Configuración global
const API_BASE_URL = 'http://localhost:8000';
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
// 2. ESTADO DEL PROYECTO (Donut Chart) - INTERACTIVO
// =======================================================
async function renderProjectStatus() {
  try {
    const response = await fetch(`${API_BASE_URL}/api/project/status`);
    const data = await response.json();

    const container = d3.select('#project-status-chart');
    container.html('');

    if (!Array.isArray(data) || data.length === 0) {
      container.append('div').attr('class', 'no-data-message')
        .text('No hay datos de tareas para el estado del proyecto.');
      return;
    }

    const totalTasks = data.reduce((sum, d) => sum + (Number(d.count) || 0), 0);
    const width = 450, height = 300, outerRadius = 120, innerRadius = outerRadius * 0.6;

    const svg = container.append('svg')
      .attr('width', width)
      .attr('height', height)
      .append('g')
      .attr('transform', `translate(${outerRadius + 20}, ${height / 2})`);

    const pie = d3.pie().value(d => d.count).sort(null);
    const arc = d3.arc().innerRadius(innerRadius).outerRadius(outerRadius);

    const arcs = svg.selectAll('.arc')
      .data(pie(data))
      .enter().append('g')
      .attr('class', 'arc');

    arcs.append('path')
      .attr('d', arc)
      .attr('fill', d => statusColors[String(d.data.status).toUpperCase()] || '#cccccc')
      .style('cursor', 'pointer')
      .on('click', function (event, d) {
        const status = String(d.data.status).toUpperCase();
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
          .html(`<strong>${d.data.status}</strong>: ${d.data.count} tareas (${d3.format(".1%")(d.data.count / Math.max(1, totalTasks))})`)
          .style('left', (event.pageX + 10) + 'px')
          .style('top', (event.pageY - 28) + 'px');
      })
      .on('mouseout', function () {
        d3.select('#tooltip').style('opacity', 0);
      });

    arcs.append('text')
      .attr('transform', d => `translate(${arc.centroid(d)})`)
      .attr('dy', '0.35em')
      .text(d => d3.format(".1%")(d.data.count / Math.max(1, totalTasks)))
      .style('text-anchor', 'middle')
      .style('fill', 'white')
      .style('font-size', '12px');

    renderLegend(svg, data, totalTasks, outerRadius);
  } catch (error) {
    console.error("Error al renderizar el estado del proyecto:", error);
    d3.select('#project-status-chart').html(`<div class="error-message">Error cargando datos: ${error.message}</div>`);
  }
}

function renderLegend(svg, data, totalTasks, outerRadius) {
  const legendOffset = outerRadius + 50;
  const legendSpacing = 20;

  const legend = svg.selectAll(".legend")
    .data(data.map(d => ({ _id: d._id || d.status, count: d.count })))
    .enter().append("g")
    .attr("class", "legend")
    .attr("transform", (d, i) => `translate(${legendOffset}, ${i * legendSpacing - (data.length * legendSpacing) / 2 + 10})`);

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
    .text(d => `${String(d._id).replace('_', ' ')} - ${((d.count / Math.max(1, totalTasks)) * 100).toFixed(1)}%`);
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
// 3. CARGA DE TRABAJO (Bar Chart) - INTERACTIVO
// =======================================================
async function renderWorkloadChart() {
  try {
    const response = await fetch(`${API_BASE_URL}/api/tasks/gantt`);
    let data = await response.json();

    const container = d3.select('#workload-chart');
    container.html('');

    if (!Array.isArray(data) || data.length === 0) {
      container.append('div').attr('class', 'no-data-message')
        .text('No hay datos de tareas para la carga de trabajo.');
      renderWorkloadLegend(container); 
      return;
    }

    const statusKeys = Object.keys(statusColors).filter(s => s !== 'COMPLETED' && s !== 'CANCELLED');
    const dataByStatus = d3.group(data.filter(d => statusKeys.includes(String(d.status).toUpperCase())), d => String(d.assigned_user_id || 'N/A'));

    let processedData = Array.from(dataByStatus, ([raw_user_id, tasks]) => { 
      const display_user_id = raw_user_id === 'N/A' ? 'Sin Asignar' : `Usuario ${raw_user_id}`; 
      const userEntry = { 
        raw_user_id: raw_user_id, 
        display_user_id: display_user_id 
      };
      statusKeys.forEach(status => {
        userEntry[status] = tasks.filter(t => String(t.status).toUpperCase() === status).length;
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
          // El valor para 'Sin Asignar' es la cadena vacía, que coincide con el <option value="">Todos los usuarios</option>
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
        const statusKey = String(task.status ?? 'TO_DO').toUpperCase();
        const color = statusKey === 'TO_DO' || statusKey === 'IN_PROGRESS' || statusKey === 'BLOCKED' ? statusColors['TO_DO'] : statusColors['CANCELLED'];

        container.append('div')
            .attr('class', `task-item overdue`)
            .attr('style', `border-left: 5px solid ${color};`)
            .html(`
                <div style="display: flex; justify-content: space-between; align-items: center; width: 100%;">
                    <div style="font-weight: 600; font-size: 16px; flex-grow: 1;">
                        <i class="fas fa-exclamation-triangle" style="color: ${color};"></i>
                        <span class="task-title">${task.name || task.title}</span>
                    </div>
                    
                    <div style="text-align:right; margin-left: 10px; min-width: 100px;">
                        <div class="user-badge" style="font-size: 14px; background-color: ${color}; color: white; padding: 2px 6px; border-radius: 4px; display: inline-block;">
                            Usuario ${task.assigned_user_id || task.assigned_to || 'N/A'}
                        </div>
                        <div class="task-date" style="margin-top:6px; font-size: 14px;">
                            Venció hace 
                            <span style="font-weight: bold; color: ${statusColors['TO_DO']}; display: block;">${task.days_overdue} días</span>
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
// 5. SCOREBOARD DE EFICIENCIA POR RECURSO
// =======================================================
async function calculateAndRenderEfficiencyScoreboard() {
    try {
        const response = await fetch(`${API_BASE_URL}/api/tasks/gantt`);
        const data = await response.json();
        
        const container = d3.select('#efficiency-scoreboard');
        container.html('');
        
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
            const statusKey = String(task.status ?? 'TO_DO').toUpperCase();
            
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
                .text('No hay tareas activas o completadas asignadas a recursos.');
            return;
        }

        const table = container.append('table')
            .attr('class', 'efficiency-table')
            .style('width', '100%');

        const header = table.append('thead').append('tr');
        header.append('th').text('Recurso').style('text-align', 'left');
        header.append('th').text('Asignadas').attr('title', 'Total de tareas activas o completadas').style('text-align', 'center');
        header.append('th').text('Completadas').style('text-align', 'center');
        header.append('th').text('Tasa (%)').attr('title', 'Tasa de Completitud').style('text-align', 'center');
        header.append('th').text('Vencidas').style('text-align', 'center');

        const body = table.append('tbody');

        finalData.forEach(d => {
            const completionRate = d.completion_rate;
            let rateColor = statusColors['IN_PROGRESS']; 
            
            if (completionRate >= 80) {
                rateColor = statusColors['COMPLETED']; 
            } else if (completionRate < 50) {
                rateColor = statusColors['BLOCKED']; 
            }
            if (completionRate === 0 && d.total_tasks > 0) {
                rateColor = statusColors['TO_DO']; 
            }

            const overdueColor = (d.overdue_tasks > 0) ? statusColors['TO_DO'] : statusColors['COMPLETED']; 

            const row = body.append('tr').attr('class', 'efficiency-row');
            
            row.append('td')
                .html(`Usuario <strong>${d.user_id === 'N/A' ? 'Sin Asignar' : d.user_id}</strong>`);
                
            row.append('td')
                .text(d.total_tasks)
                .style('text-align', 'center')
                .style('font-size', '16px') // <--- CAMBIO DE TAMAÑO DE LETRA
                .style('font-weight', 'bold'); // <--- AÑADIDO NEGRITA
                
            row.append('td')
                .text(d.completed_tasks)
                .style('text-align', 'center')
                .style('font-size', '16px') // <--- CAMBIO DE TAMAÑO DE LETRA
                .style('font-weight', 'bold'); // <--- AÑADIDO NEGRITA
                
            row.append('td')
                .text(`${completionRate.toFixed(1)}%`)
                .style('color', rateColor)
                .style('font-weight', 'bold')
                 .style('font-size', '16px') 
                .style('text-align', 'center')
                
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
// 6. TAREAS PRÓXIMAS (REINTEGRADO)
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
    
    // Simplificamos la etiqueta para cualquier fecha más allá de una semana,
    // ya que ahora la lista de tareas no tiene límite temporal.
    if (diffDays > 7) return 'Más adelante'; 
    
    return 'N/A'; // Tareas vencidas, pero ya filtradas por la función llamante
}

async function getUpcomingTasks() {
    try {
        const response = await fetch(`${API_BASE_URL}/api/tasks/gantt`);
        const data = await response.json();
        
        const today = new Date();
        today.setHours(0, 0, 0, 0);

        // Eliminamos el límite de 30 días para mostrar todas las tareas próximas.
        
        const upcomingTasks = data.filter(task => {
            const statusKey = String(task.status ?? 'TO_DO').toUpperCase();
            if (statusKey === 'COMPLETED' || statusKey === 'CANCELLED') return false;

            const dueDate = task.end_date || task.due_date;
            const parsedDate = parseDateFlexible(dueDate);

            if (parsedDate) {
                // MODIFICACIÓN: Solo verificamos que la fecha no haya vencido (>= hoy)
                return parsedDate >= today; 
            }
            return false;
        }).sort((a, b) => {
            const dateA = parseDateFlexible(a.end_date || a.due_date);
            const dateB = parseDateFlexible(b.end_date || b.due_date);
            return (dateA?.getTime() ?? Infinity) - (dateB?.getTime() ?? Infinity);
        });

        // Mantenemos el límite a 10 tareas para no saturar el widget.
        return upcomingTasks.slice(0, 10); 
    } catch (error) {
        console.error("Error fetching upcoming tasks:", error);
        return [];
    }
}

async function renderDailyTasks() {
    const data = await getUpcomingTasks();
    const container = d3.select('#daily-tasks');
    container.html('');

    if (data.length === 0) {
        container.append('div').attr('class', 'no-data-message')
            .text('No hay tareas activas con fecha próxima.');
        return;
    }

    data.forEach(task => {
        const dueDate = parseDateFlexible(task.end_date || task.due_date);
        const relativeDate = dueDate ? getRelativeDate(dueDate) : 'Sin Fecha';
        const user = task.assigned_user_id || task.assigned_to || 'N/A';
        const taskTitle = task.name || task.title || 'Tarea sin nombre';

        const color = statusColors[String(task.status).toUpperCase()] || '#cccccc';

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
// 8. GANTT (con diagnóstico y zoom por botones)
// =======================================================
async function renderGanttChart() {
  try {
    const statusFilterElement = document.getElementById('status-filter');
    const userFilterElement = document.getElementById('user-filter');

    const url = new URL(`${API_BASE_URL}/api/tasks/gantt`);
    if (statusFilterElement && statusFilterElement.value) url.searchParams.append('status', statusFilterElement.value);
    // userFilterElement.value solo será NO vacío si se seleccionó un ID de usuario válido que fue previamente cargado.
    if (userFilterElement && userFilterElement.value) url.searchParams.append('user_id', userFilterElement.value); 

    const response = await fetch(url.toString());
    if (!response.ok) throw new Error(`Error HTTP ${response.status}: ${response.statusText}`);
    let data = await response.json();

    const container = d3.select('#gantt-chart');
    container.html('');
    container.style('display', 'block');

    if (!Array.isArray(data)) throw new Error("Formato de datos incorrecto: no es un array");

    let validTasks = [];
    data.forEach(task => {
      const startDate = task.start_date ? new Date(task.start_date) : null;
      let endDate = task.end_date ? new Date(task.end_date) : null;
      const isInvalid = !startDate || isNaN(startDate.getTime());

      if (isInvalid) return;

      if (!endDate || isNaN(endDate.getTime())) {
        endDate = new Date(startDate);
        endDate.setDate(startDate.getDate() + 7);
      }

      if (endDate < startDate) {
        const minimumEndDate = new Date(startDate);
        minimumEndDate.setDate(startDate.getDate() + 1);
        endDate = minimumEndDate;
      }

      task.start = startDate;
      task.end = endDate;
      task.task_id = task.task_id || task.name || task.title; 
      validTasks.push(task);
    });

    data = validTasks.sort((a, b) => a.start - b.start);

    if (data.length === 0) {
      container.append('div').attr('class', 'no-data-message p-4')
        .text('No hay tareas con fechas de inicio y fin válidas para los filtros seleccionados.');
      return;
    }

    const margin = { top: 25, right: 30, bottom: 60, left: 150 };
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
        datePadding = 14; tickFormat = d3.timeFormat("Sem %U\n%b %d"); tickCount = Math.ceil(width / 100); break;
      case 'months':
        datePadding = 60; tickFormat = d3.timeFormat("%b %Y"); tickCount = Math.ceil(width / 120); break;
      default:
        datePadding = 2; tickFormat = d3.timeFormat("%a %d"); tickCount = Math.ceil(width / 70);
    }

    dateRange[0] = d3.timeDay.offset(d3.timeDay.floor(dateRange[0]), -datePadding);
    dateRange[1] = d3.timeDay.offset(d3.timeDay.ceil(dateRange[1]), datePadding);

    const x = d3.scaleTime().domain(dateRange).range([0, width]);
    const y = d3.scaleBand().domain(data.map(d => d.task_id)).range([0, height]).padding(0.2);
    const barColor = status => statusColors[String(status).toUpperCase()] || '#cccccc';

    svg.selectAll('.task-bar')
      .data(data)
      .enter().append('rect')
      .attr('class', 'task-bar')
      .attr('y', d => y(d.task_id))
      .attr('height', y.bandwidth())
      .attr('x', d => Math.max(0, x(d.start)))
      .attr('width', d => {
        const startX = Math.max(0, x(d.start));
        const endX = x(d.end);
        return Math.max(1, endX - startX);
      })
      .attr('fill', d => barColor(d.status))
      .attr('rx', 3)
      .attr('ry', 3)
      .on('mouseover', function (event, d) {
        d3.select('#tooltip')
          .style('opacity', 1)
          .html(`
            <div class="tooltip-content">
              <strong>${d.name || d.title}</strong><br>
              <small>ID: ${d.task_id}</small><br>
              <hr>
              Inicio: ${d.start.toLocaleDateString()}<br>
              Fin: ${d.end.toLocaleDateString()}<br>
              Duración: ${Math.ceil((d.end - d.start) / (1000 * 60 * 60 * 24))} días<br>
              Asignado: Usuario ${d.assigned_user_id || 'N/A'}<br>
              Estado: ${d.status}
            </div>
          `)
          .style('left', (event.pageX + 15) + 'px')
          .style('top', (event.pageY - 15) + 'px');
      })
      .on('mouseout', function () {
        d3.select('#tooltip').style('opacity', 0);
      });

    svg.selectAll('.bar-label')
      .data(data.filter(d => (x(d.end) - x(d.start)) > 100))
      .enter().append('text')
      .attr('class', 'bar-label')
      .attr('x', d => x(d.start) + 5)
      .attr('y', d => y(d.task_id) + y.bandwidth() / 2)
      .attr('dy', '.35em')
      .text(d => d.name || d.title)
      .attr('fill', 'white')
      .style('font-size', '12px');

    svg.append('g').attr('class', 'x-axis').attr('transform', `translate(0, ${height})`).call(d3.axisBottom(x).ticks(tickCount).tickFormat(d3.timeFormat(tickFormat)))
      .selectAll("text")
      .style("text-anchor", "start")
      .attr("transform", "rotate(45)")
      .attr("dx", "1em")
      .attr("dy", "1em");
      
    svg.append('g').attr('class', 'y-axis').call(d3.axisLeft(y)).selectAll(".tick text").text(d => {
      const task = data.find(t => t.task_id === d);
      return `Usuario ${task.assigned_user_id || 'N/A'}: ${d.length > 20 ? d.substring(0, 17) + '...' : d}`;
    });
    
    // Línea de hoy
    const today = new Date();
    today.setHours(0, 0, 0, 0);
    if (x.domain()[0] <= today && x.domain()[1] >= today) {
        const todayX = x(today);

        svg.append('line')
          .attr('class', 'today-line')
          .attr('x1', todayX)
          .attr('x2', todayX)
          .attr('y1', 0)
          .attr('y2', height)

        svg.append('text')
          .attr('class', 'today-text')
          .attr('x', todayX)
          .attr('y', -5)
          .attr('text-anchor', 'middle')
          .text('HOY');
    }
    
    // Diagnóstico
    const errors = data.filter(d => d.status.toUpperCase() !== 'COMPLETED' && d.end < today);
    if (errors.length > 0) {
        d3.select('#gantt-diagnostic').html(`<i class="fas fa-exclamation-triangle" style="color: ${statusColors['TO_DO']};"></i> <strong>${errors.length} tareas</strong> vencidas y no completadas.`);
    } else {
        d3.select('#gantt-diagnostic').html(`<i class="fas fa-check-circle" style="color: ${statusColors['COMPLETED']};"></i> No hay tareas vencidas en esta vista.`);
    }

    // Asegurar scroll horizontal si el contenido es demasiado ancho
    setTimeout(() => {
        const svgWidth = svg.node().parentElement.getBoundingClientRect().width;
        const containerWidthParent = container.node().parentElement.clientWidth;
        if (svgWidth > containerWidthParent) container.style('overflow-x', 'auto');
    }, 50);

  } catch (error) {
    console.error("Error al renderizar el Gantt:", error);
    d3.select('#gantt-chart').html(`<div class="error-message">Error cargando el gráfico de Gantt: ${error.message}</div>`);
    d3.select('#gantt-diagnostic').html(`<i class="fas fa-exclamation-circle" style="color: ${statusColors['TO_DO']};"></i> Error en la carga de datos.`);
  }
}

// =======================================================
// 9. POBLAR FILTRO DE USUARIOS (NUEVO)
// =======================================================
async function populateUserFilter() {
    try {
        // Usamos el endpoint de gantt para obtener todos los datos de tareas y extraer los usuarios
        const response = await fetch(`${API_BASE_URL}/api/tasks/gantt`);
        const data = await response.json();
        
        const userFilter = document.getElementById('user-filter');
        if (!userFilter) return;

        // Limpiar opciones existentes (excepto "Todos los usuarios")
        userFilter.innerHTML = '<option value="">Todos los usuarios</option>';

        // Recopilar IDs únicos de usuarios (ignorando 'N/A' y vacíos)
        const uniqueUsers = new Set();
        data.forEach(task => {
            // Aseguramos que el ID se obtenga de 'assigned_user_id' o 'assigned_to' y se limpie
            const userId = String(task.assigned_user_id || task.assigned_to || '').trim();
            if (userId && userId !== 'N/A') {
                uniqueUsers.add(userId);
            }
        });

        // Ordenar y añadir opciones al select
        const sortedUsers = Array.from(uniqueUsers).sort((a, b) => a.localeCompare(b, undefined, { numeric: true }));

        sortedUsers.forEach(userId => {
            const option = document.createElement('option');
            option.value = userId;
            option.textContent = `Usuario ${userId}`;
            userFilter.appendChild(option);
        });

    } catch (error) {
        console.error("Error al poblar el filtro de usuarios:", error);
    }
}


// =======================================================
// 10. FUNCIÓN DE CARGA INICIAL
// =======================================================
async function loadAllData() {
  const loadingDiv = document.getElementById('loading-overlay');
  if (loadingDiv) loadingDiv.style.display = 'flex';

  // Carga paralela de todos los componentes
  await Promise.all([
    renderMetrics(),
    renderProjectStatus(),
    renderWorkloadChart(),
    renderOverdueTasks(),
    calculateAndRenderEfficiencyScoreboard(),
    renderDailyTasks(),
    populateUserFilter(), // Llamada a la nueva función para llenar el <select>
    renderGanttChart()
  ]);

  lastUpdateTime = new Date().toLocaleTimeString();
  document.getElementById('last-update-time').textContent = lastUpdateTime;

  if (loadingDiv) loadingDiv.style.display = 'none';
}


// Inicialización
document.addEventListener('DOMContentLoaded', () => {
    // 1. Tooltip global 
    if (!document.getElementById('tooltip')) {
        d3.select('body').append('div')
            .attr('id', 'tooltip')
            .style('position', 'absolute')
            .style('opacity', 0)
            .style('background-color', 'rgba(255, 255, 255, 0.95)')
            .style('border', '1px solid #ddd')
            .style('padding', '8px')
            .style('border-radius', '4px')
            .style('pointer-events', 'none')
            .style('z-index', 99999); 
    }

    // 2. Control de carga CSV
    const ingestBtn = document.getElementById('ingest-csv-btn');
    if (ingestBtn) {
        ingestBtn.addEventListener('click', handleIngestCsv);
    }

    // 3. Controles de filtrado (usando la función resetFiltersAndRefresh expuesta globalmente)
    const statusFilter = document.getElementById('status-filter');
    if (statusFilter) statusFilter.addEventListener('change', renderGanttChart);

    const userFilter = document.getElementById('user-filter');
    if (userFilter) userFilter.addEventListener('change', renderGanttChart);
    
    // 4. Carga inicial de datos
    loadAllData();
    
    // 5. Establecer intervalo de refresco (ej. cada 60 segundos)
    setInterval(loadAllData, 60000); 
});

// LISTO 10