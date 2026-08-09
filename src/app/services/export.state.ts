import { Service, signal } from '@angular/core';
import { form } from '@angular/forms/signals';

export type ExportType = "bitmap" | "json";
export type BitmapFormat = "png";
export type BitmapDisposition = "sat-hua.lig" | "sat-hut.lig";

export interface ExportData {
    type: ExportType,
    format: BitmapFormat,
    disposition: BitmapDisposition,
    onlyDrawPalette: boolean
}

interface ExportStateModel {
    export: ExportData
}

@Service()
export class ExportState {        

    state = signal<ExportStateModel>({
        export: {
            type: 'bitmap',
            format: 'png',
            disposition: 'sat-hua.lig',
            onlyDrawPalette: true
        },
    })

    stateForm = form(
        this.state
    )
}
