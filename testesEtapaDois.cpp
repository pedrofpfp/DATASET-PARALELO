#include <iostream>
#include <fstream>
#include <sstream>   // NOVO: Para capturar saída por coluna em paralelo
#include <vector>
#include <string>
#include <chrono>
#include <omp.h>     // NOVO: OpenMP

// =======================================================================
// PASSO 1: DEFINIÇÃO DE ESTRUTURAS MANUAIS (Sem usar std::map)
// =======================================================================

struct CategoriaFreq {
    std::string nome;
    size_t contagem;
};

struct NumericoFreq {
    double valor;
    size_t contagem;
};

struct ColunaStats {
    std::string nome;
    bool is_numerica = true;
    size_t valores_nulos = 0;
    std::vector<double> valores_numericos;
    std::vector<CategoriaFreq> distribuicao_categorica;
};

// =======================================================================
// PASSO 2: FUNÇÕES AUXILIARES MANUAIS
// =======================================================================

std::string trim(const std::string& str) {
    size_t inicio = 0;
    while (inicio < str.length() &&
           (str[inicio] == ' ' || str[inicio] == '\t' ||
            str[inicio] == '\r' || str[inicio] == '\n')) {
        inicio++;
    }
    if (inicio >= str.length()) return "";
    size_t fim = str.length() - 1;
    while (fim > inicio &&
           (str[fim] == ' ' || str[fim] == '\t' ||
            str[fim] == '\r' || str[fim] == '\n')) {
        fim--;
    }
    return str.substr(inicio, fim - inicio + 1);
}

std::vector<std::string> dividirString(const std::string& linha, char delimitador) {
    std::vector<std::string> colunas;
    size_t inicio = 0;
    size_t pos = linha.find(delimitador);
    while (pos != std::string::npos) {
        colunas.push_back(linha.substr(inicio, pos - inicio));
        inicio = pos + 1;
        pos = linha.find(delimitador, inicio);
    }
    colunas.push_back(linha.substr(inicio));
    return colunas;
}

bool tentarConverterDouble(const std::string& str, double& out_val) {
    std::string limpa = trim(str);
    if (limpa.empty()) return false;
    try {
        size_t pos;
        out_val = std::stod(limpa, &pos);
        return pos == limpa.length();
    } catch (...) {
        return false;
    }
}

void registrarCategoria(std::vector<CategoriaFreq>& distribuicao,
                        const std::string& valor) {
    for (size_t i = 0; i < distribuicao.size(); ++i) {
        if (distribuicao[i].nome == valor) {
            distribuicao[i].contagem++;
            return;
        }
    }
    distribuicao.push_back({valor, 1});
}

// -----------------------------------------------------------------------
// Quicksort com paralelismo via tasks (threshold evita overhead em arrays pequenos)
// -----------------------------------------------------------------------
void ordenarValores(std::vector<double>& arr, int esquerda, int direita) {
    if (esquerda >= direita) return;
    double pivo = arr[(esquerda + direita) / 2];
    int i = esquerda, j = direita;
    while (i <= j) {
        while (arr[i] < pivo) i++;
        while (arr[j] > pivo) j--;
        if (i <= j) {
            double temp = arr[i]; arr[i] = arr[j]; arr[j] = temp;
            i++; j--;
        }
    }

    // Cria tasks paralelas apenas para partições grandes o suficiente
    // para compensar o overhead de criação de task
    if ((direita - esquerda) > 2000) {
        #pragma omp task shared(arr)
        ordenarValores(arr, esquerda, j);

        #pragma omp task shared(arr)
        ordenarValores(arr, i, direita);

        #pragma omp taskwait
    } else {
        ordenarValores(arr, esquerda, j);
        ordenarValores(arr, i, direita);
    }
}

// Invólucro que cria a região paralela para as tasks do quicksort.
// Chamado fora de regiões paralelas ativas (dentro de seções 'single').
void ordenarValoresParalelo(std::vector<double>& arr) {
    if (arr.size() < 2) return;
    #pragma omp parallel
    {
        #pragma omp single nowait
        ordenarValores(arr, 0, (int)arr.size() - 1);
    }
}

double calcularRaizQuadrada(double numero) {
    if (numero <= 0) return 0;
    double est = numero, eps = 0.000001;
    while ((est - numero / est) > eps)
        est = (est + numero / est) / 2.0;
    return est;
}

// =======================================================================
// PASSO 3: CÁLCULOS ESTATÍSTICOS — saída via std::ostream& para
//          permitir captura paralela sem races na impressão
// =======================================================================

void exibirMetricasNumericas(ColunaStats& col,
                              size_t total_linhas,
                              std::ostream& out) {
    out << "\n[Numerica] " << col.nome << "\n";
    double perc_nulos = total_linhas > 0
                        ? (col.valores_nulos * 100.0 / total_linhas) : 0;
    out << "  - Valores Nulos/Ausentes: " << col.valores_nulos
        << " (" << perc_nulos << "%)\n";

    if (col.valores_numericos.empty()) return;
    size_t n = col.valores_numericos.size();

    // ----- Soma (redução paralela) -----
    double soma = 0.0;
    #pragma omp parallel for reduction(+:soma) schedule(static) if(n > 10000)
    for (int i = 0; i < (int)n; ++i)
        soma += col.valores_numericos[i];

    double media = soma / n;

    // ----- Variância (redução paralela) -----
    double soma_variancia = 0.0;
    #pragma omp parallel for reduction(+:soma_variancia) schedule(static) if(n > 10000)
    for (int i = 0; i < (int)n; ++i) {
        double d = col.valores_numericos[i] - media;
        soma_variancia += d * d;
    }
    double desvio_padrao = calcularRaizQuadrada(soma_variancia / n);

    // ----- Tabela de frequências (necessária para a Moda) -----
    std::vector<NumericoFreq> frequencias;
    for (size_t i = 0; i < n; ++i) {
        double v = col.valores_numericos[i];
        bool encontrado = false;
        for (size_t j = 0; j < frequencias.size(); ++j) {
            if (frequencias[j].valor == v) { frequencias[j].contagem++; encontrado = true; break; }
        }
        if (!encontrado) frequencias.push_back({v, 1});
    }

    // ----- Ordenação paralela (tasks) -----
    ordenarValoresParalelo(col.valores_numericos);

    double min_val  = col.valores_numericos[0];
    double max_val  = col.valores_numericos[n - 1];
    double amplitude = max_val - min_val;
    double q1       = col.valores_numericos[n / 4];
    double q3       = col.valores_numericos[(n * 3) / 4];
    double mediana  = (n % 2 == 0)
                      ? (col.valores_numericos[n/2-1] + col.valores_numericos[n/2]) / 2.0
                      : col.valores_numericos[n / 2];

    double moda = col.valores_numericos[0];
    size_t max_freq = 0;
    for (size_t i = 0; i < frequencias.size(); ++i) {
        if (frequencias[i].contagem > max_freq) {
            max_freq = frequencias[i].contagem;
            moda     = frequencias[i].valor;
        }
    }

    out << "  - Registos validos: " << n             << "\n"
        << "  - Media:         "    << media          << "\n"
        << "  - Minimo:        "    << min_val         << "\n"
        << "  - Q1 (25%):      "    << q1              << "\n"
        << "  - Mediana (Q2):  "    << mediana         << "\n"
        << "  - Q3 (75%):      "    << q3              << "\n"
        << "  - Maximo:        "    << max_val         << "\n"
        << "  - Amplitude:     "    << amplitude       << "\n"
        << "  - Moda:          "    << moda
                                    << " (aparece " << max_freq << " vezes)\n"
        << "  - Desvio Padrao: "    << desvio_padrao   << "\n";
}

void exibirMetricasCategoricas(const ColunaStats& col,
                                size_t total_linhas,
                                std::ostream& out) {
    out << "\n[Categorica] " << col.nome << "\n";
    double perc_nulos = total_linhas > 0
                        ? (col.valores_nulos * 100.0 / total_linhas) : 0;
    out << "  - Valores Nulos/Ausentes: " << col.valores_nulos
        << " (" << perc_nulos << "%)\n";

    size_t cardinalidade = col.distribuicao_categorica.size();
    out << "  - Cardinalidade: " << cardinalidade << " valores unicos\n";
    if (cardinalidade == 0) return;

    size_t total_validos = total_linhas - col.valores_nulos;
    out << "  - Distribuicao (Frequencia Relativa e Absoluta):\n";
    for (size_t i = 0; i < cardinalidade; ++i) {
        double freq_rel = (col.distribuicao_categorica[i].contagem * 100.0)
                          / total_validos;
        out << "      \"" << col.distribuicao_categorica[i].nome << "\": "
            << col.distribuicao_categorica[i].contagem
            << " (" << freq_rel << "%)\n";
    }
}

// =======================================================================
// PASSO 4: FUNÇÃO PRINCIPAL
// =======================================================================

int main() {
    const std::string caminho_arquivo = "globalterrorismdb_0718dist.csv";
    const char        delimitador     = ',';
    const bool        tem_cabecalho   = true;

    std::cout << "Passo 1: Iniciando. Threads disponiveis: "
              << omp_get_max_threads() << "\n";

    auto t0 = std::chrono::high_resolution_clock::now();

    // ===================================================================
    // FASE 1 — Leitura sequencial: carrega todas as linhas na memória
    // (I/O de disco é inerentemente sequencial)
    // ===================================================================
    std::cout << "Passo 2: Abrindo '" << caminho_arquivo << "'.\n";
    std::ifstream arquivo(caminho_arquivo);
    if (!arquivo.is_open()) {
        std::cerr << "ERRO: Nao foi possivel abrir o arquivo.\n";
        return 1;
    }

    std::vector<std::string> cabecalho;
    std::vector<std::vector<std::string>> todas_as_linhas; // todas as linhas de dados
    size_t total_bytes = 0;

    std::cout << "Passo 3: Lendo linhas do arquivo...\n";
    {
        std::string linha;
        bool primeira = true;
        while (std::getline(arquivo, linha)) {
            if (linha.empty()) continue;
            total_bytes += linha.length();
            std::vector<std::string> campos = dividirString(linha, delimitador);

            if (primeira) {
                if (tem_cabecalho) {
                    for (auto& c : campos) cabecalho.push_back(trim(c));
                } else {
                    todas_as_linhas.push_back(std::move(campos));
                    for (size_t i = 0; i < cabecalho.size() == 0 ? todas_as_linhas[0].size() : 0; ++i)
                        cabecalho.push_back("Coluna " + std::to_string(i + 1));
                }
                primeira = false;
                continue; // pula o cabeçalho
            }
            todas_as_linhas.push_back(std::move(campos));
        }
    }
    arquivo.close();

    const size_t num_colunas        = cabecalho.size();
    const size_t total_linhas_dados = todas_as_linhas.size();

    std::cout << "  -> " << total_linhas_dados << " linhas de dados, "
              << num_colunas << " colunas lidas.\n";

    // ===================================================================
    // FASE 2 — Processamento paralelo por coluna
    //
    // Cada thread fica responsável por um subconjunto de colunas.
    // Como cada ColunaStats é acessada por apenas uma thread de cada vez,
    // não há race conditions nem necessidade de locks.
    // ===================================================================
    std::cout << "Passo 4: Processando colunas em paralelo...\n";

    std::vector<ColunaStats> dataset_stats(num_colunas);
    for (size_t i = 0; i < num_colunas; ++i)
        dataset_stats[i].nome = cabecalho[i];

    #pragma omp parallel for schedule(dynamic) shared(dataset_stats, todas_as_linhas)
    for (int col_idx = 0; col_idx < (int)num_colunas; ++col_idx) {
        ColunaStats& col = dataset_stats[col_idx];

        for (size_t row = 0; row < total_linhas_dados; ++row) {
            const auto& campos = todas_as_linhas[row];
            if (col_idx >= (int)campos.size()) {
                col.valores_nulos++;
                continue;
            }

            std::string valor = trim(campos[col_idx]);

            if (valor.empty()) {
                col.valores_nulos++;
                continue;
            }

            if (col.is_numerica) {
                double val;
                if (tentarConverterDouble(valor, val)) {
                    col.valores_numericos.push_back(val);
                } else {
                    // Coluna reclassificada como categórica: migra valores já guardados
                    col.is_numerica = false;
                    for (size_t k = 0; k < col.valores_numericos.size(); ++k) {
                        char buf[50];
                        snprintf(buf, sizeof(buf), "%g", col.valores_numericos[k]);
                        registrarCategoria(col.distribuicao_categorica, std::string(buf));
                    }
                    col.valores_numericos.clear();
                    col.valores_numericos.shrink_to_fit();
                    registrarCategoria(col.distribuicao_categorica, valor);
                }
            } else {
                registrarCategoria(col.distribuicao_categorica, valor);
            }
        }
    }  // fim parallel for — barreira implícita garante que todos terminaram

    // ===================================================================
    // FASE 3 — Cálculo das estatísticas e captura da saída em paralelo
    //
    // Cada thread escreve no seu próprio ostringstream (sem races).
    // Ao final, imprimimos na ordem original (sequencial e determinístico).
    // ===================================================================
    std::cout << "Passo 5: Calculando estatisticas...\n";

    std::vector<std::ostringstream> saidas(num_colunas);

    #pragma omp parallel for schedule(dynamic) shared(dataset_stats, saidas)
    for (int i = 0; i < (int)num_colunas; ++i) {
        if (dataset_stats[i].is_numerica)
            exibirMetricasNumericas(dataset_stats[i],
                                    total_linhas_dados, saidas[i]);
        else
            exibirMetricasCategoricas(dataset_stats[i],
                                      total_linhas_dados, saidas[i]);
    }

    // Impressão sequencial — preserva a ordem das colunas
    std::cout << "\n=== RELATORIO ESTATISTICO DO DATASET ===\n";
    std::cout << "Total de linhas de dados: " << total_linhas_dados << "\n";
    for (size_t i = 0; i < num_colunas; ++i)
        std::cout << saidas[i].str();

    // ===================================================================
    // Métricas de desempenho
    // ===================================================================
    auto t1 = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed = t1 - t0;

    double tamanho_medio = total_linhas_dados > 0
                           ? ((double)total_bytes / total_linhas_dados) : 0;

    std::cout << "\n=== METRICAS DE DESEMPENHO E SISTEMA ===\n"
              << "Threads utilizadas:               " << omp_get_max_threads()     << "\n"
              << "Tempo Total (Leitura + Calculos): " << elapsed.count()           << " segundos\n"
              << "Total de dados em texto lido:     " << total_bytes               << " Bytes\n"
              << "Tamanho Medio da Linha:           " << tamanho_medio             << " Bytes por linha\n";

    return 0;
}