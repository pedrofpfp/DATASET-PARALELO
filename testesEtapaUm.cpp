#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <chrono>

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
    size_t valores_nulos = 0; // NOVO: Rastreamento de Valores Ausentes
    std::vector<double> valores_numericos; 
    std::vector<CategoriaFreq> distribuicao_categorica; 
};

// =======================================================================
// PASSO 2: FUNÇÕES AUXILIARES MANUAIS (Substituindo bibliotecas)
// =======================================================================

std::string trim(const std::string& str) {
    size_t inicio = 0;
    while (inicio < str.length() && (str[inicio] == ' ' || str[inicio] == '\t' || str[inicio] == '\r' || str[inicio] == '\n')) {
        inicio++;
    }
    size_t fim = str.length() - 1;
    while (fim > inicio && (str[fim] == ' ' || str[fim] == '\t' || str[fim] == '\r' || str[fim] == '\n')) {
        fim--;
    }
    if (inicio > fim) return "";
    return str.substr(inicio, fim - inicio + 1);
}

std::vector<std::string> dividirString(const std::string& linha, char delimitador) {
    std::vector<std::string> colunas;
    size_t inicio = 0;
    size_t pos_delimitador = linha.find(delimitador);
    
    while (pos_delimitador != std::string::npos) {
        colunas.push_back(linha.substr(inicio, pos_delimitador - inicio));
        inicio = pos_delimitador + 1;
        pos_delimitador = linha.find(delimitador, inicio);
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

void registrarCategoria(std::vector<CategoriaFreq>& distribuicao, const std::string& valor) {
    for (size_t i = 0; i < distribuicao.size(); ++i) {
        if (distribuicao[i].nome == valor) {
            distribuicao[i].contagem++;
            return;
        }
    }
    distribuicao.push_back({valor, 1});
}

void ordenarValores(std::vector<double>& arr, int esquerda, int direita) {
    if (esquerda >= direita) return;
    double pivo = arr[(esquerda + direita) / 2];
    int i = esquerda, j = direita;
    while (i <= j) {
        while (arr[i] < pivo) i++;
        while (arr[j] > pivo) j--;
        if (i <= j) {
            double temp = arr[i];
            arr[i] = arr[j];
            arr[j] = temp;
            i++;
            j--;
        }
    }
    ordenarValores(arr, esquerda, j);
    ordenarValores(arr, i, direita);
}

double calcularRaizQuadrada(double numero) {
    if (numero <= 0) return 0;
    double estimativa = numero;
    double margem_erro = 0.000001;
    while ((estimativa - numero / estimativa) > margem_erro) {
        estimativa = (estimativa + numero / estimativa) / 2.0;
    }
    return estimativa;
}

// =======================================================================
// PASSO 3: CÁLCULOS ESTATÍSTICOS (Lógica de Negócio Atualizada)
// =======================================================================

void exibirMetricasNumericas(ColunaStats& col, size_t total_linhas) {
    std::cout << "\n[Numerica] " << col.nome << std::endl;
    double perc_nulos = total_linhas > 0 ? (col.valores_nulos * 100.0 / total_linhas) : 0;
    std::cout << "  - Valores Nulos/Ausentes: " << col.valores_nulos << " (" << perc_nulos << "%)" << std::endl;

    if (col.valores_numericos.empty()) return;
    size_t n = col.valores_numericos.size();

    double soma = 0;
    std::vector<NumericoFreq> frequencias;
    
    for (size_t i = 0; i < n; ++i) {
        double v = col.valores_numericos[i];
        soma += v;
        
        bool encontrado = false;
        for (size_t j = 0; j < frequencias.size(); ++j) {
            if (frequencias[j].valor == v) {
                frequencias[j].contagem++;
                encontrado = true;
                break;
            }
        }
        if (!encontrado) frequencias.push_back({v, 1});
    }

    double media = soma / n;

    double soma_variancia = 0;
    for (size_t i = 0; i < n; ++i) {
        double diferenca = col.valores_numericos[i] - media;
        soma_variancia += (diferenca * diferenca);
    }
    double desvio_padrao = calcularRaizQuadrada(soma_variancia / n);

    // Ordenação necessária para Mediana, Quartis, Min, Max e Amplitude
    ordenarValores(col.valores_numericos, 0, n - 1);
    
    // NOVO: Min, Max e Amplitude
    double min_val = col.valores_numericos[0];
    double max_val = col.valores_numericos[n - 1];
    double amplitude = max_val - min_val;

    // NOVO: Quartis (Q1 e Q3)
    double q1 = col.valores_numericos[n / 4];
    double q3 = col.valores_numericos[(n * 3) / 4];

    double mediana = 0; // (Q2)
    if (n % 2 == 0) {
        mediana = (col.valores_numericos[n / 2 - 1] + col.valores_numericos[n / 2]) / 2.0;
    } else {
        mediana = col.valores_numericos[n / 2];
    }

    double moda = col.valores_numericos[0];
    size_t max_freq = 0;
    for (size_t i = 0; i < frequencias.size(); ++i) {
        if (frequencias[i].contagem > max_freq) {
            max_freq = frequencias[i].contagem;
            moda = frequencias[i].valor;
        }
    }

    std::cout << "  - Registos validos: " << n << std::endl;
    std::cout << "  - Media:         " << media << std::endl;
    std::cout << "  - Minimo:        " << min_val << std::endl;
    std::cout << "  - Q1 (25%):      " << q1 << std::endl;
    std::cout << "  - Mediana (Q2):  " << mediana << std::endl;
    std::cout << "  - Q3 (75%):      " << q3 << std::endl;
    std::cout << "  - Maximo:        " << max_val << std::endl;
    std::cout << "  - Amplitude:     " << amplitude << std::endl;
    std::cout << "  - Moda:          " << moda << " (aparece " << max_freq << " vezes)" << std::endl;
    std::cout << "  - Desvio Padrao: " << desvio_padrao << std::endl;
}

void exibirMetricasCategoricas(const ColunaStats& col, size_t total_linhas) {
    std::cout << "\n[Categorica] " << col.nome << std::endl;
    double perc_nulos = total_linhas > 0 ? (col.valores_nulos * 100.0 / total_linhas) : 0;
    std::cout << "  - Valores Nulos/Ausentes: " << col.valores_nulos << " (" << perc_nulos << "%)" << std::endl;
    
    // NOVO: Cardinalidade
    size_t cardinalidade = col.distribuicao_categorica.size();
    std::cout << "  - Cardinalidade: " << cardinalidade << " valores unicos" << std::endl;

    if (cardinalidade == 0) return;
    
    size_t total_validos = total_linhas - col.valores_nulos;

    std::cout << "  - Distribuicao (Frequencia Relativa e Absoluta):" << std::endl;
    for (size_t i = 0; i < cardinalidade; ++i) {
        // NOVO: Frequência Relativa (%)
        double freq_relativa = (col.distribuicao_categorica[i].contagem * 100.0) / total_validos;
        std::cout << "      \"" << col.distribuicao_categorica[i].nome << "\": " 
                  << col.distribuicao_categorica[i].contagem 
                  << " (" << freq_relativa << "%)" << std::endl;
    }
}

// =======================================================================
// PASSO 4: FUNÇÃO PRINCIPAL (Controle de Fluxo)
// =======================================================================

int main() {
    std::string caminho_arquivo = "globalterrorismdb_0718dist.csv";
    char delimitador = ','; 
    bool ignorar_primeira_linha_como_cabecalho = true;

    std::vector<ColunaStats> dataset_stats;
    size_t total_linhas_processadas = 0;
    size_t total_bytes_processados = 0; // NOVO: Métrica de sistema

    std::cout << "Passo 1: Iniciando o programa. Configurando bibliotecas basicas." << std::endl;
    auto inicio_tempo = std::chrono::high_resolution_clock::now();

    std::cout << "Passo 2: Tentando abrir o arquivo '" << caminho_arquivo << "'." << std::endl;
    std::ifstream arquivo(caminho_arquivo);
    if (!arquivo.is_open()) {
        std::cerr << "ERRO: Nao foi possivel abrir o arquivo." << std::endl;
        return 1;
    }

    std::string linha;
    bool primeira_linha = true;

    std::cout << "Passo 3: Lendo o arquivo linha a linha..." << std::endl;
    while (std::getline(arquivo, linha)) {
        if (linha.empty()) continue;

        // Adiciona o tamanho da linha (em bytes) à nossa métrica de sistema
        total_bytes_processados += linha.length();

        std::vector<std::string> colunas = dividirString(linha, delimitador);

        if (primeira_linha) {
            std::cout << "  -> Configurando estruturas de colunas baseado na linha 1." << std::endl;
            for (size_t i = 0; i < colunas.size(); ++i) {
                ColunaStats nova_coluna;
                if (ignorar_primeira_linha_como_cabecalho) {
                    nova_coluna.nome = trim(colunas[i]);
                } else {
                    nova_coluna.nome = "Coluna " + std::to_string(i + 1);
                }
                dataset_stats.push_back(nova_coluna);
            }
            primeira_linha = false;
            if (ignorar_primeira_linha_como_cabecalho) continue;
        }

        total_linhas_processadas++;

        for (size_t i = 0; i < colunas.size() && i < dataset_stats.size(); ++i) {
            ColunaStats& col = dataset_stats[i];
            std::string valor_limpo = trim(colunas[i]);

            // NOVO: Controle de Valores Ausentes/Nulos
            if (valor_limpo.empty()) {
                col.valores_nulos++;
                continue; // Pula o processamento para não interferir na matemática
            }

            if (col.is_numerica) {
                double val;
                if (tentarConverterDouble(valor_limpo, val)) {
                    col.valores_numericos.push_back(val);
                } else {
                    col.is_numerica = false;
                    for (size_t j = 0; j < col.valores_numericos.size(); ++j) {
                        char buffer[50];
                        snprintf(buffer, sizeof(buffer), "%g", col.valores_numericos[j]);
                        registrarCategoria(col.distribuicao_categorica, std::string(buffer));
                    }
                    col.valores_numericos.clear();
                    col.valores_numericos.shrink_to_fit(); 
                    
                    registrarCategoria(col.distribuicao_categorica, valor_limpo);
                }
            } else {
                registrarCategoria(col.distribuicao_categorica, valor_limpo);
            }
        }
    }

    arquivo.close();
    std::cout << "Passo 4: Arquivo lido completamente e fechado. Processando matematica..." << std::endl;
    
    std::cout << "\n=== RELATORIO ESTATISTICO DO DATASET ===" << std::endl;
    std::cout << "Total de linhas de dados: " << total_linhas_processadas << std::endl;

    for (size_t i = 0; i < dataset_stats.size(); ++i) {
        if (dataset_stats[i].is_numerica) {
            exibirMetricasNumericas(dataset_stats[i], total_linhas_processadas);
        } else {
            exibirMetricasCategoricas(dataset_stats[i], total_linhas_processadas);
        }
    }

    auto fim_tempo = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> tempo_decorrido = fim_tempo - inicio_tempo;

    std::cout << "\n=== METRICAS DE DESEMPENHO E SISTEMA (BASELINE) ===" << std::endl;
    std::cout << "Tempo Total (Leitura + Calculos Manuais): " << tempo_decorrido.count() << " segundos" << std::endl;
    
    // NOVO: Tamanho Médio da Linha
    double tamanho_medio = total_linhas_processadas > 0 ? ((double)total_bytes_processados / total_linhas_processadas) : 0;
    std::cout << "Total de dados em texto lido: " << total_bytes_processados << " Bytes" << std::endl;
    std::cout << "Tamanho Medio da Linha: " << tamanho_medio << " Bytes por linha" << std::endl;

    return 0;
}